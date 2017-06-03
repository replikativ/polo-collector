(defproject polo-collector "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]

                 [cheshire "5.7.0"]
                 [io.replikativ/konserve-leveldb "0.1.1"]
                 [io.replikativ/replikativ "0.2.4-SNAPSHOT"]
                 [io.replikativ/replikativ-fressianize "0.1.0-SNAPSHOT"]
                 [employeerepublic/slf4j-timbre "0.4.2"]]

  :profiles {:dev {:dependencies [[plotly-clj "0.1.1"]]}}

  :main polo-collector.core)
