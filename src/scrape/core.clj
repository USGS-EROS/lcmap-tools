(ns scrape.core
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [org.httpkit.client :as client]
            [net.cgrand.enlive-html :as html]))

(defn get-links
  "Get all links at URL as absolute URLs."
  [url]
  (printf "getting all links at %s" url)
  (->> ;; first get all <a href="">...</a> elements...
       (-> (java.net.URL. url)
           (html/html-resource)
           (html/select [:a]))
       ;; ...then pluck out the href attributes...
       (map :attrs)
       (map :href)
       ;; ...and make an absolute URL.
       (map #(str url %))))

(defn get-body
  "GET body of URL after delay"
  ([url delay]
   (Thread/sleep delay)
   (println "getting body for %s" url)
   (-> url client/get deref :body slurp))
  ([url]
   (get-body url 500)))

(defn base->batch
  "Get links to batches of scenes."
  [base-url]
  (printf "getting list of batches at URL")
  (->> (get-links base-url)
       (rest)))

(defn batch->scenes
  "Get links to scene/md5 URL pairs."
  [scene-list-url]
  (printf "getting list of scenes for %s" scene-list-url)
  (->> (get-links scene-list-url)
       (rest)
       (partition 2)))

(defn scene->source
  "Build a 'source' map for a scene/md5 URL pair."
  [[checksum-url scene-url]]
  (printf "building source for %s" scene-url)
  (let [content (get-body checksum-url)
        [_ checksum scene-id] (re-matches #"([\S]+) ([\S]+)" content)]
    {:id scene-id
     :uri scene-url
     :checksum checksum}))

(defn save
  ""
  [path data]
  (printf "saving to %s" path)
  (spit path (prn-str data)))

(defn put-source!
  ""
  [source-url source-data]
  (printf "putting source: %s" (:id source-data))
  (-> @(client/put (str source-url (:id source-data))
                   {:headers {"Accept" "application/json"}
                    :form-params source-data})
      (:body)
      (json/decode)))

(defn save-batch-info [batches ix]
  (let [save-as (format "data/batch-%s.edn" ix)
        batch (nth batches ix)]
    (def batch (nth batch-list ix))
    (def sources (map scene->source (batch->scenes batch)))
    (save save-as (doall sources))))

(comment
  (def base-url "https://edclpdsftp.cr.usgs.gov/downloads/lcmap/sites/washington/")
  (def batch-list (base->batch base-url))
  (save-batch-info batch-list 1)
  (doall (map (partial save-batch-info batch-list) (range 17 48)))
  #_(def app-url "http://localhost:5678/landsat/source/")
  #_(map (partial put-source! app-url) source-list)
  #_(map save-batch-info (take 1 batch-list) (iterate inc 1)))
