(ns cuckoolog.movie-ratings
  (:use cascalog.api)
  (:require [clojure.string :as s]
            [cascalog.ops :as c])
  (:gen-class))

;; ## Utilities

(def less?
  "Accepts two Comparables and returns true if the first is strictly
   less than the second."
  (comp neg? compare))

(defn try-parse-float
  "Attempts to create a float by parsing the supplied string."
  [s]
  (try (Float/parseFloat s)
       (catch Throwable _)))

(defn square
  "Returns the square of the supplied number."
  [x]
  (* x x))

(def square-xs
  "Parallel cascalog squaring operator. Used to square multiple values
   in the same predicate."
  (c/each #'square))

;; ## Similarity Statistics

(defn correlation
  [size dot-prod rating-a-sum rating-b-sum rating-a-norm-sq rating-b-norm-sq]
  (let [sqrt-diff   (fn [norm-sq rating-sum]
                      (Math/sqrt (- (* size norm-sq)
                                    (square rating-sum))))
        numerator   (- (* size dot-prod)
                       (* rating-a-sum rating-b-sum))
        denominator (* (sqrt-diff rating-a-norm-sq rating-a-sum)
                       (sqrt-diff rating-b-norm-sq rating-b-sum))]
    (/ numerator denominator)))

(defn regularized-correlation
  [unregularized-corr size virtual-count prior-correlation]
  (let [w (/ size (+ size virtual-count))]
    (+ (* w unregularized-corr)
       (* (- 1 w) prior-correlation))))

(defn cosine-similarity
  [dot-product a-norm b-norm]
  (/ dot-product (* a-norm b-norm)))

(defn jaccard-similarity
  [users-in-common total-users-a total-users-b]
  (let [union (-> total-users-a
                  (+ total-users-b)
                  (- users-in-common))]
    (/ users-in-common union)))

(defn similarities
  [size dot-prod rating-a-sum rating-b-sum rating-a-norm-sq rating-b-norm-sq
   a-raters b-raters prior-count prior-correlation]
  (let [corr     (correlation size dot-prod
                              rating-a-sum rating-b-sum
                              rating-a-norm-sq rating-b-norm-sq)
        reg-corr (regularized-correlation corr size prior-count prior-correlation)
        cos-sim  (cosine-similarity dot-prod
                                    (Math/sqrt rating-a-norm-sq)
                                    (Math/sqrt rating-b-norm-sq))
        jaccard  (jaccard-similarity size a-raters b-raters)]
    [corr reg-corr cos-sim jaccard]))

;; ## Cascalog Queries

(defn rating-counts
  "Accepts a generator of <user, item, rating> and returns a subquery
  that produces 2-tuples of the form <item, # of raters>"
  [src min-raters max-raters]
  (<- [?item ?raters]
      (src _ ?item _)
      (<= ?raters max-raters)
      (>= ?raters min-raters)
      (c/count ?raters)))

(def prod-squares
  "Predicate macro. Accepts two ratings and returns a number of
   metrics based on the rating values."
  (<- [?ra ?rb :> ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq]
      (* ?ra ?rb :> ?prod)
      (square-xs ?ra ?rb :> ?ra-sq ?rb-sq)
      (c/sum ?prod ?ra ?rb ?ra-sq ?rb-sq
             :> ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq)))

(defn joined-query
  "Accepts a generator of <user, item, rating> and returns a subquery
  that generates a number of metrics for each pair of items rated by
  at least one user."
  [src min-intersection]
  (<- [?item-a ?item-b ?size ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq]
      (src ?user ?item-a ?ra)
      (src ?user ?item-b ?rb)
      (less? ?item-a ?item-b)
      (>= ?size min-intersection)
      (prod-squares ?ra ?rb :> ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq)
      (c/count ?size)))

(defn movie-ratings
  [joined-src rater-src prior-count prior-corr]
  (<- [?item-a ?item-b ?corr ?reg-corr ?cos-sim ?jaccard ?size ?a-raters ?b-raters]
      (rater-src ?item-a ?a-raters)
      (rater-src ?item-b ?b-raters)
      (joined-src ?item-a ?item-b ?size ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq)
      (similarities ?size ?dot-prod ?ra-sum ?rb-sum ?ra-norm-sq ?rb-norm-sq
                    ?a-raters ?b-raters prior-count prior-corr
                    :> ?corr ?reg-corr ?cos-sim ?jaccard)
      (:distinct false)))

(defn -main
  [input-path output-path & {:as opts}]
  (let [{:keys [min-intersection min-raters max-raters prior-count prior-corr]} (merge defaults opts)
        text-src   (hfs-textline input-path)
        rating-src (<- [?user ?item ?rating]
                       (text-src ?textline)
                       (s/replace ?textline "\"" "" :> ?text)
                       (s/split ?text #";" :> ?user ?item ?rating-text)
                       (try-parse-float ?rating-text :> ?rating))
        joined-src (joined-query rating-src min-intersection)
        rater-src  (rating-counts rating-src min-raters max-raters)]
    (?- (hfs-seqfile output-path :sinkmode :replace)
        (movie-ratings joined-src rater-src prior-count prior-corr))))
