(ns polo-collector.client
  (:use [plotly-clj.core])
  (:require [polo-collector.core :refer [user cdvcs-id]]
            [replikativ.crdt.cdvcs.realize :as r]
            [replikativ.peer :refer [client-peer]]
            [replikativ.p2p.fetch :refer [fetch]]
            [replikativ.stage :refer [create-stage! connect!]]
            [replikativ.crdt.cdvcs.stage :as cs]
            [konserve.filestore :refer [new-fs-store]]
            [konserve-leveldb.core :refer [new-leveldb-store]]
            [clojure.core.async :as async]
            [superv.async :refer [<?? S]]
            [replikativ.stage :as s]
            [konserve.core :as k]
            [taoensso.timbre :as timbre]
            [replikativ.crdt.cdvcs.stage :as cs]
            #_[datomic.api :as d]))


(timbre/set-level! :info)

(comment
  ;; causes NPE in slf4j (?)
  (timbre/merge-config! {:ns-whitelist ["replikativ.*"]})

  )




;; replikativ
(def client-store (<?? S (new-leveldb-store "/media/christian/05E6-1B0F/polo-collector-store")))


(def client (<?? S (client-peer S client-store :middleware fetch)))



(def client-stage (<?? S (create-stage! user client)))
(<?? S (cs/create-cdvcs! client-stage :id cdvcs-id))


(comment

  (<?? S (connect! client-stage "ws://aphrodite.polyc0l0r.net:9096"))
  )

(count (get-in @client-stage [user cdvcs-id :state :commit-graph]))



;; stream data

(def events (atom []))

(def atom-stream
  (r/stream-into-identity! client-stage [user cdvcs-id]
                           {'add-events (fn [S old evs]
                                          (go-try S
                                                  (->> (<? S (unfressianize S client-store evs))
                                                       (filter (fn [[_ t _ _ [p]]]
                                                                 (and (= t "ticker")
                                                                      (= "BTC_ETH" p))))
                                                       (map (fn [[_ t _ _ [_ last]]] (Double/parseDouble last)))
                                                       doall
                                                       (swap! old conj)))
                                          old)}
                           events))

(count (apply concat @events))

(take 10 @events)

(defn btc-eth []
  (->> @events
       (apply concat)
       (mapv (fn [[_ t _ _ e]] e))))


(count (btc-eth))


(-> (plotly (apply concat @events))
    add-scatter
    (save-html "plotly.html" :open))

(require '[clojure.core.matrix :as mat])

(let [hist (apply concat @events)]
  (/ (reduce + hist)
     (count hist)))

;; TODO
;; Reconstruct Polo BTC_ETH graph
;; plotly https://plot.ly/javascript/candlestick-charts/
;; https://github.com/andredumas/techan.js


(async/close! (:close-ch atom-stream))
