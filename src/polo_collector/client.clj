(ns polo-collector.client
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

(keys (get-in @client-stage [user cdvcs-id :state]))




(def events (atom []))

(def atom-stream (r/stream-into-identity! client-stage [user cdvcs-id]
                                          {'add-events (fn [old evs]
                                                         (when (< (rand)
                                                                  (/ 10000
                                                                     (* 7 7 24 360)))
                                                           (swap! old into evs))
                                                         old)}
                                          events))


(first @events)

(async/close! (:close-ch atom-stream))
