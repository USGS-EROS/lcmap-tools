(ns lcmap.tools
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
  (let [writer (clojure.java.io/writer path)]
    (clojure.pprint/pprint data writer)))

(defn save-batch-info
  ""
  [batches ix]
  (let [path (format "data/batch-%02d.edn" ix)
        batch (nth batches ix)
        sources (map scene->source (batch->scenes batch))]
    (save path (doall sources))))

(defn pretty-source
  ""
  [ix]
  (-> (slurp (format "data/batch-%s.edn" ix))
      (edn/read-string)
      (clojure.pprint/pprint (clojure.java.io/writer (format "data/pretty-%s.edn" ix)))))

(defn put-source!
  ""
  [source-url source-data]
  (printf "putting source: %s" (:id source-data))
  (-> @(client/put (str source-url (:id source-data))
                   {:headers {"Accept" "application/json"}
                    :form-params source-data})
      (:body)
      (json/decode)))

;; A pattern for matching the scene-identifier from a URL.
(def yoink #"\/([A-Z0-9-]+)\.tar.gz")

;; A pattern for matching a mission-path-row, get the first match .
(def mission-path-row #"(LT04[0-9]{6})|(LT05[0-9]{6})|(LE07[0-9]{6})|(LC8[0-9]{6})")

(defn path-row
  "Exctract path-row from `source` :uri"
  [source]
  (first (re-find mission-path-row (source :uri))))

(defn make-source
  "Turn a checksum and path into a `source` data-structure`"
  [[checksum path]]
  {:id (last (re-find yoink path))
   :checksum checksum
   :uri (str "https://edclpdsftp.cr.usgs.gov/downloads/lcmap/sites/chesapeake/" path)})

(defn make-inventory
  "Turn a list of checksum / data URLs into `source` data-structure"
  [text]
  (->> text
       (re-seq #"\S+")
       (partition 2)
       (map #(make-source %))))

(comment
  "Chesapeake bay, batched by mission-path-row"
  (def chesapeake-inventory (doall (make-inventory (slurp "https://edclpdsftp.cr.usgs.gov/downloads/lcmap/sites/chesapeake/output.txt"))))
  (def chesapeake-grouped (group-by path-row chesapeake-inventory))
  (pmap (fn [[path-row sources]] (save (format "chesapeake/%s.edn" path-row) sources)) chesapeake-grouped))

(comment
  "Washington state, batched by mission-path-row"
  (def washington-inventory (doall (make-inventory (slurp "https://edclpdsftp.cr.usgs.gov/downloads/lcmap/sites/washington/output.txt"))))
  (def washington-grouped (group-by path-row washington-inventory))
  (pmap (fn [[path-row sources]] (save (format "washington/%s.edn" path-row) sources)) washington-grouped))
