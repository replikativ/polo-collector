(ns polo-collector.core
  (:require [clojure.core.async :refer [chan timeout]]
            [kabel
             [peer :refer [start stop] :as peer]]
            [kabel.middleware.wamp :as wamp]
            [kabel.middleware.json :refer [json]]
            [konserve
             [filestore :refer [new-fs-store delete-store]]
             [memory :refer [new-mem-store]]]
            [konserve-leveldb.core :refer [new-leveldb-store]]
            [replikativ
             [peer :refer [server-peer client-peer]]
             [stage :refer [connect! create-stage!]]]
            [replikativ.crdt.cdvcs.stage :as cs]
            [replikativ.stage :as s]
            [taoensso.timbre :as timbre]
            [superv.async :refer [go-try <? <?? go-loop-try S
                                  restarting-supervisor]]
            [konserve.core :as k]))

(timbre/set-level! :info)

(def user "mail:polo@crawler.com") ;; will be used to authenticate you (not yet)

(def cdvcs-id #uuid "312c5d5f-a849-4b08-b80e-64f47d3532a7")



(defn new-event [pending event]
  (swap! pending (fn [[prev cur] event] [prev (conj cur event)])
         (conj event (java.util.Date.))))

(defn store-events [stage pending]
  (go-try S
   (let [st (.getTime (java.util.Date.))
         events (vec (first (swap! pending (fn [[prev cur]] [cur '()]))))
         event-txs #_(mapv (fn [t] ['add-event t]) events)
         [['add-events events]]]
     (when-not (empty? events)
       (<? S (cs/transact! stage [user cdvcs-id] event-txs))
       ;; print a bit of stats from time to time
       (when (< (rand) 0.05)
         (println "Date: " (java.util.Date.))
         (println "Pending: " (count (second @pending)))
         (println "Commit count:" (count (get-in @stage [user cdvcs-id :state :commit-graph])))
         (println "Time taken: " (- (.getTime (java.util.Date.))
                                    st) " ms")
         (println "Free Memory: " (.freeMemory (Runtime/getRuntime))
                  " of "
                  (.totalMemory (Runtime/getRuntime)))
         (println "Transaction count: " (count event-txs))
         (println "First event:" (first events)))))))

(defn -main [store-path & topics]
  #_(delete-store store-path)
  (println "Tracking topics:" topics)
  ;; defing here for simple API access on the REPL, use Stuart Sierras component in larger systems
  (let [_ (def store (<?? S (new-leveldb-store store-path)))
        _ (def peer (<?? S (server-peer S store "ws://127.0.0.1:9096")))
        ;; TODO use a queue
        _ (def pending (atom ['() '()]))
        _ (start peer)
        _ (def stage (<?? S (create-stage! user peer)))
        _ (<?? S (cs/create-cdvcs! stage :id cdvcs-id))
        c (chan)]
    (go-loop-try S []
                 (<? S (store-events stage pending))
                 (<? S (timeout (* 10 1000)))
                 (recur))
    (def polo-client (peer/client-peer S #uuid "898dcf36-e07a-4338-92fd-f818d573444a"
                                       (partial wamp/wamp-middleware
                                                topics
                                                "realm1"
                                                #(new-event pending %))
                                       json))
    (restarting-supervisor
     (fn [S]
       (go-try S
         (<? S (peer/connect S polo-client "wss://api.poloniex.com/")))))
    ;; we def things here, so we can independently stop and start the stream from the REPL
    ;; HACK block main thread
    (<?? S c)))


(comment
  (-main "/tmp/polo-test" "ticker" "trollbox" "BTC_XMR" "BTC_ETH")


  )

