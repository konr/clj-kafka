(ns clj-kafka.test.utils
  (:import  [kafka.common TopicExistsException])
  (:require [expectations    :refer :all]
            [clj-kafka.utils :refer :all]))

(defn random-channel-name []
  (str "test-channel-" (java.util.UUID/randomUUID)))

(let [channel (random-channel-name)
      zk-client (zookeeper-client {:zookeeper-host "localhost" :zookeeper-port 2181})]

  (given (create-topic zk-client channel)
         (expect identity channel))

  (given (create-topic zk-client channel)
         (expect identity nil))

  (given (try (create-topic! zk-client channel)
              (catch Exception e e))
         (expect class TopicExistsException)))
