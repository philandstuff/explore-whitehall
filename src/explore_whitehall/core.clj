(ns explore-whitehall.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.instant :as inst]
            [incanter.charts :as charts]
            [incanter.core :as incanter]
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
   (not val-str) nil
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
       (incanter/$ (keyword "@timestamp") data)))

(defn parse-stream "parse a stream of rails log messages" [data]
  (incanter/$map parse-message (keyword "@message") data))

(defn decorate-data
  "decorate a dataset with the parsed fields from @message and parsed timestamps from @timestamp"
  [data]
  (incanter/conj-cols (parse-stream data)
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

(comment
  (incanter/view (chart-column :duration (decorate-data normal-data)))
  (incanter/view (chart-column :db (decorate-data normal-data)))
  (incanter/view (chart-column :view (decorate-data normal-data)))
  (incanter/view (chart-columns [:duration :db :view] (decorate-data normal-data)))
  )

(comment
  (incanter/view normal-data)

  (incanter/view (incanter/conj-cols (incanter/sel normal-data :except-cols (keyword "@message"))
                                     (incanter/$map parse-message (keyword "@message") normal-data)))
  )

