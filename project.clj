(defproject polo-collector "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire "5.7.0"]
                 [org.clojure/core.async "1.2.603"]
                 [http-kit "2.4.0-alpha6"] 
                 [io.replikativ/superv.async "0.3.43"]
                 [io.replikativ/kabel "0.2.3-SNAPSHOT"]
                 [io.replikativ/konserve "0.7.275"]
                 #_[io.replikativ/konserve-leveldb "0.1.1"]
                 #_[io.replikativ/replikativ "0.2.4-SNAPSHOT"]
                 #_[io.replikativ/replikativ-fressianize "0.1.0-SNAPSHOT"]
                 [org.clojure/tools.reader "1.3.6"]
                 [com.taoensso/timbre "4.10.0"]
                 #_[employeerepublic/slf4j-timbre "0.4.2"]]

  :jvm-opts ["--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]

  :profiles {:dev {:dependencies [[plotly-clj "0.1.1"]]}}

  :main polo-collector.core)
