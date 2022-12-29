(ns polo-collector.core
  (:require [clojure.core.async :refer [chan timeout]]
            [kabel
             [peer :refer [start stop] :as peer]]
            [kabel.middleware.wamp :as wamp]
            [kabel.middleware.json :refer [json]]
            [konserve
             [filestore :refer [connect-fs-store delete-store]]
             [memory :refer [new-mem-store]]]
            #_[replikativ
             [peer :refer [server-peer client-peer]]
             [stage :refer [connect! create-stage!]]]
            #_[replikativ.crdt.cdvcs.stage :as cs]
            #_[replikativ.stage :as s]
            #_[replikativ-fressianize.core :refer [fressianize]]
            [taoensso.timbre :as timbre]
            [clojure.core.async :refer [timeout go go-loop <! >! >!! put! chan] :as async]
            [superv.async :refer [go-try <? <?? go-loop-try S >? put? <?- <??-
                                  restarting-supervisor]]
            [konserve.core :as k]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]))


(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

;; fix java version parsing bug in http-kit SSL support
(alter-var-root #'org.httpkit.sni-client/java-version_ (fn [_] (delay 14)))

(timbre/set-level! :info)

(defn new-event [pending event]
  (swap! pending (fn [[prev cur] event] [prev (conj cur event)])
         (assoc event :date (java.util.Date.))))

(defn store-events [store pending]
  (go-try S
   (let [st (.getTime (java.util.Date.))
         events (vec (first (swap! pending (fn [[prev cur]] [cur '()]))))
         event-txs #_(mapv (fn [t] ['add-event t]) events)
         [['add-events events]]]
     (when-not (empty? events)
       #_(<? S (cs/transact! stage [user cdvcs-id] (fressianize event-txs)))
       ;; print a bit of stats from time to time
       (<?- (k/assoc store st event-txs))
       (println "Value written at key: " st)
       (when (< (rand) 0.05)
         (println "Date: " (java.util.Date.))
         (println "Pending: " (count (second @pending)))
         #_(println "Commit count:" (count (get-in @stage [user cdvcs-id :state :commit-graph])))
         (println "Time taken: " (- (.getTime (java.util.Date.))
                                    st) " ms")
         (println "Free Memory: " (.freeMemory (Runtime/getRuntime))
                  " of "
                  (.totalMemory (Runtime/getRuntime)))
         (println "Transaction count: " (count event-txs))
         (println "First event:" (first events)))))))


(defn polo-dispatch [msg]
  #_(println "dispatch:" msg)
  (if (or (vector? msg)
          (seq? msg))
    (first msg)
    :other))


(def polo-currency-pairs
  (->>
   '{
     ;; copy and pasted from polo API docs
     177 BTC_ARDR
     253 	BTC_ATOM
     324 	BTC_AVA
     210 	BTC_BAT
     189 	BTC_BCH
     236 	BTC_BCHABC
     238 	BTC_BCHSV
     7 	BTC_BCN
     232 	BTC_BNT
     14 	BTC_BTS
     269 	BTC_BTT
     15 	BTC_BURST
     194 	BTC_CVC
     24 	BTC_DASH
     162 	BTC_DCR
     27 	BTC_DOGE
     201 	BTC_EOS
     171 	BTC_ETC
     148 	BTC_ETH
     266 	BTC_ETHBNT
     246 	BTC_FOAM
     317 	BTC_FXC
     198 	BTC_GAS
     185 	BTC_GNT
     251 	BTC_GRIN
     43 	BTC_HUC
     207 	BTC_KNC
     275 	BTC_LINK
     213 	BTC_LOOM
     250 	BTC_LPT
     163 	BTC_LSK
     50 	BTC_LTC
     229 	BTC_MANA
     295 	BTC_MATIC
     302 	BTC_MKR
     309 	BTC_NEO
     64 	BTC_NMC
     248 	BTC_NMR
     69 	BTC_NXT
     196 	BTC_OMG
     249 	BTC_POLY
     75 	BTC_PPC
     221 	BTC_QTUM
     174 	BTC_REP
     170 	BTC_SBD
     150 	BTC_SC
     204 	BTC_SNT
     290 	BTC_SNX
     168 	BTC_STEEM
     200 	BTC_STORJ
     89 	BTC_STR
     182 	BTC_STRAT
     312 	BTC_SWFTC
     92 	BTC_SYS
     263 	BTC_TRX
     108 	BTC_XCP
     112 	BTC_XEM
     114 	BTC_XMR
     117 	BTC_XRP
     277 	BTC_XTZ
     178 	BTC_ZEC
     192 	BTC_ZRX
     306 	DAI_BTC
     307 	DAI_ETH
     211 	ETH_BAT
     190 	ETH_BCH
     202 	ETH_EOS
     172 	ETH_ETC
     176 	ETH_REP
     179 	ETH_ZEC
     193 	ETH_ZRX
     284 	PAX_BTC
     285 	PAX_ETH
     326 	TRX_AVA
     271 	TRX_BTT
     267 	TRX_ETH
     319 	TRX_FXC
     316 	TRX_JST
     276 	TRX_LINK
     297 	TRX_MATIC
     311 	TRX_NEO
     292 	TRX_SNX
     274 	TRX_STEEM
     314 	TRX_SWFTC
     273 	TRX_WIN
     268 	TRX_XRP
     279 	TRX_XTZ
     254 	USDC_ATOM
     235 	USDC_BCH
     237 	USDC_BCHABC
     239 	USDC_BCHSV
     224 	USDC_BTC
     256 	USDC_DASH
     243 	USDC_DOGE
     257 	USDC_EOS
     258 	USDC_ETC
     225 	USDC_ETH
     252 	USDC_GRIN
     244 	USDC_LTC
     242 	USDC_STR
     264 	USDC_TRX
     226 	USDC_USDT
     241 	USDC_XMR
     240 	USDC_XRP
     245 	USDC_ZEC
     288 	USDJ_BTC
     323 	USDJ_BTT
     289 	USDJ_TRX
     255 	USDT_ATOM
     325 	USDT_AVA
     212 	USDT_BAT
     191 	USDT_BCH
     260 	USDT_BCHABC
     298 	USDT_BCHBEAR
     259 	USDT_BCHSV
     299 	USDT_BCHBULL
     320 	USDT_BCN
     280 	USDT_BEAR
     293 	USDT_BSVBEAR
     294 	USDT_BSVBULL
     121 	USDT_BTC
     270 	USDT_BTT
     281 	USDT_BULL
     304 	USDT_BVOL
     308 	USDT_DAI
     122 	USDT_DASH
     262 	USDT_DGB
     216 	USDT_DOGE
     203 	USDT_EOS
     330 	USDT_EOSBEAR
     329 	USDT_EOSBULL
     173 	USDT_ETC
     149 	USDT_ETH
     300 	USDT_ETHBEAR
     301 	USDT_ETHBULL
     318 	USDT_FXC
     217 	USDT_GNT
     261 	USDT_GRIN
     305 	USDT_IBVOL
     315 	USDT_JST
     322 	USDT_LINK
     332 	USDT_LINKBEAR
     331 	USDT_LINKBULL
     218 	USDT_LSK
     123 	USDT_LTC
     231 	USDT_MANA
     296 	USDT_MATIC
     303 	USDT_MKR
     310 	USDT_NEO
     124 	USDT_NXT
     286 	USDT_PAX
     223 	USDT_QTUM
     175 	USDT_REP
     219 	USDT_SC
     291 	USDT_SNX
     321 	USDT_STEEM
     125 	USDT_STR
     313 	USDT_SWFTC
     265 	USDT_TRX
     282 	USDT_TRXBEAR
     283 	USDT_TRXBULL
     287 	USDT_USDJ
     272 	USDT_WIN
     126 	USDT_XMR
     127 	USDT_XRP
     328 	USDT_XRPBEAR
     327 	USDT_XRPBULL
     278 	USDT_XTZ
     180 	USDT_ZEC
     220 	USDT_ZRX

     }
   (map (fn [[k v]]
          [k (keyword (name v))]))
   (into {})))

(defn poloniex-middleware [pending subscriptions [S peer [in out]]]
  (let [new-in (chan)
        new-out (chan)
        p (async/pub in polo-dispatch)
        ]

    (doseq [s subscriptions]
      (let [sub-ch (chan)
            _ (async/sub p s sub-ch)]
        (go-try S
          (>! out {:command :subscribe
                   :channel s})
          (println {:event ::subscribed
                    :channel s}))
        (go-loop-try S [e (<?- sub-ch)]
          (when e
            #_(println "Channel " s ":" e)
            (new-event pending {:api :poloniex
                                :event e})
            (recur (<?- sub-ch))))))
    [S peer [new-in new-out]]))

(defn bybit-middleware [pending topics [S peer [in out]]]
  (let [new-in  (chan)
        new-out (chan)]
    (put! out {:op :subscribe
               :args topics})
    (go-loop-try S [e (<?- in)]
                 (when e
                   (new-event pending {:api   :bybit
                                       :event e})
                   (recur (<?- in))))
    (go-loop-try S []
                 (>! out {:op :ping})
                 (<?- (timeout 10000))
                 (recur))
    [S peer [new-in new-out]]))

(defn GET [url options]
  (let [res-ch (chan)]
    (http/get url options
              (fn [{:keys [status headers body error]}]
                (if error
                  (put! res-ch (ex-info "GET failed." {:type ::get-failed
                                                       :url url
                                                       :error error}))
                  (put! res-ch {:status status
                                :headers headers
                                :body (try
                                        (cheshire.core/parse-string body keyword)
                                        (catch Exception _
                                          body))}))))
    res-ch))


;; https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#market-data-endpoints

(def ^:dynamic *binance-base-url* "https://api.binance.com/api/v3/")

(defn binance-collector [S pending]
  (println "binance-collector starting")
  (go-loop-try S []
    (let [books (->> #{"ETHBTC" "BTCUSDT"}
                   (map #(GET (str *binance-base-url* "depth") {:query-params {:symbol %
                                                                              :limit 1000}
                                                               :timeout 10000}))
                 async/merge
                 (<? S))
          ticker (<? S (GET (str *binance-base-url* "ticker/price") {:timeout 10000}))]
      #_(when (< (rand) 0.1)
        (println books ticker))
      (new-event pending {:api :binance
                          :books books
                          :ticker ticker})
      (<! (timeout 1000))
      (recur))))



(defn -main [store-path]
  #_(delete-store store-path)
  #_(println "Tracking topics:" topics)
  ;; defing here for simple API access on the REPL, use Stuart Sierras component in larger systems
  (let [topics (concat [1002 ;; ticker
                        ]
                       (keys polo-currency-pairs))
        bybit-topics ["publicTrade.BTCUSDT"
                      "publicTrade.ETHUSDT"
                      "tickers.BTCUSDT"
                      "tickers.ETHUSDT"
                      "orderbook.200.BTCUSDT"
                      "orderbook.200.ETHUSDT"
                      ]
        _ (def store (<??- (connect-fs-store store-path :compressor konserve.compressor/lz4-compressor)))
        #_(def peer (<?? S (server-peer S store "ws://127.0.0.1:9096")))
        ;; TODO use a queue
        _ (def pending (atom ['() '()]))
        #_(start peer)
        #_(def stage (<?? S (create-stage! user peer)))
        #_(<?? S (cs/create-cdvcs! stage :id cdvcs-id))
        c (chan)]
    ;; test net connection
    #_(connect! stage "ws://replikativ.io:8888")
    (go-loop-try S []
                 (<?- (store-events store #_stage pending))
                 (<?- (timeout (* 60 1000)))
                 (recur))


    (def polo-client (peer/client-peer S #uuid "898dcf36-e07a-4338-92fd-f818d573444a"
                                       (partial poloniex-middleware pending topics)
                                       json))

    (def bybit-client (peer/client-peer S #uuid "898dcf36-e07a-4338-92fd-f818d573444a"
                                        (partial bybit-middleware pending bybit-topics)
                                        json))

    (restarting-supervisor
     (fn [S]
       (go-try S
         #_(binance-collector S pending)
         (<? S (peer/connect S bybit-client "wss://stream.bybit.com/contract/usdt/public/v3"))
         #_(<? S (peer/connect S polo-client "wss://api2.poloniex.com/")))))
    ;; we def things here, so we can independently stop and start the stream from the REPL
    ;; HACK block main thread
    #_(<?? S c)))



(comment
  (def subscriptions (concat [1002 ;; ticker
                              ]
                             (keys polo-currency-pairs)))

  (-main "/home/christian/polo-test")

  (keys (:volatile @bybit-client))

  (async/close! (first (:chans (:volatile @bybit-client))))


  (apply -main "/home/christian/polo-test" subscriptions)

  (count (second @pending))


  (def current-keys (<?? S (k/keys store)))

  (count current-keys)


  (<?? S (k/get store (:key (first current-keys))))

  ;; binance starting key with :event schema 1589620443214


)
