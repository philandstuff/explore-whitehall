(ns explore-whitehall.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.instant :as inst]
            [incanter.charts :as charts]
            [incanter.core :as i]
            [incanter.io :as iio]
            [incanter.zoo :as zoo]))

;;
;; This series of functions operates on csv datasets of rails logs
;; extracted from Kibana. A sample of the data looks like this:
;;
;; @source_host,@message,@timestamp
;; whitehall-frontend-1.frontend.production,method=GET path=/government/policies.json format=json controller=policies action=index status=200 duration=197.90 view=0.82 db=157.96,2013-07-23T10:20:10.774570Z
;; the middle field, "@message", is the key one which we want to parse.
;;
;; The function decorate-data below takes a CSV dataset and decorates
;; it with the parsed contents of the @message fields. It also parses
;; the @timestamp field into a unix integer timestamp.

(def normal-data (iio/read-dataset "/Users/philandstuff/Downloads/1015.csv" :header true))
(def outage-data (iio/read-dataset "/Users/philandstuff/Downloads/1115.csv" :header true))

(defn parse-val "detect and parse floating point numbers" [val-str]
  (cond
   (not val-str) 0
   (re-matches #"\d+\.\d*" val-str) (Double/parseDouble val-str)
   :else val-str))

(defn parse-keyval "parse key-value pairs of the form 'key=value'" [keyval]
  (let [[key val] (str/split keyval #"=")
        key (keyword key)
        val (parse-val val)]
    [key val]))

(defn parse-message "parse a rails log message into a hashmap" [msg]
  (into {}
        (map parse-keyval (str/split msg #"\s+"))))

(defn timestamps "get timestamps from logstash stream" [data]
  (map #(.getTime (inst/read-instant-timestamp %))
       (i/$ (keyword "@timestamp") data)))

(defn parse-stream "parse a stream of rails log messages" [data]
  (i/$map parse-message (keyword "@message") data))

(defn decorate-data
  "decorate a dataset with the parsed fields from @message and parsed timestamps from @timestamp"
  [data]
  (i/conj-cols (parse-stream data)
                      (map (fn [ts] {:time ts}) (timestamps data))
                      data))

(defn chart-column [colname data]
  (charts/time-series-plot
   :time
   colname
   :data data
   :y-label (str colname)
   :x-label "time"))

(defn chart-columns [colnames data]
  (let [[first-col & colnames] colnames
        chart (charts/time-series-plot
               :time first-col
               :data data
               :y-label "value"
               :x-label "time"
               :legend true
               :series-label first-col)]
    (doseq [colname colnames]
      (charts/add-lines
       chart
       :time
       colname
       :data data
       :series-label colname))
    chart))

(defn chart-rolling-medians [colnames data]
  (let [[first-col & colnames] colnames
        chart (charts/time-series-plot
               (i/$ :time data)
               (zoo/roll-median 40 (i/$ first-col data))
               :series-label first-col
               :x-label "Timestamp"
               :y-label "Duration"
               :legend true)]
    (doseq [colname colnames]
      (charts/add-lines
       chart
       (i/$ :time data)
       (zoo/roll-median 40 (i/$map #(or % 0) colname data))
       :series-label colname))
    chart))

(comment
  (i/view (chart-column :duration (i/transform-col (decorate-data normal-data) :duration #(zoo/roll-median 40 %))))
  (i/view (chart-column :db (decorate-data normal-data)))
  (i/view (chart-column :view (decorate-data normal-data)))
  (i/view (chart-columns [:duration :db :view] (decorate-data normal-data)))
  (i/view (chart-rolling-medians [:duration :view :db] (decorate-data normal-data)))
  (i/head (i/$ :duration (decorate-data normal-data)))
  )

(comment
  (i/view normal-data)

  (i/view (i/conj-cols (i/sel normal-data :except-cols (keyword "@message"))
                                     (i/$map parse-message (keyword "@message") normal-data)))
  )

