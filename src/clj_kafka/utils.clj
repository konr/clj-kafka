(ns clj-kafka.utils
  (:import  [org.I0Itec.zkclient ZkClient]
            [org.I0Itec.zkclient.serialize ZkSerializer]
            [kafka.common TopicExistsException]
            [kafka.admin CreateTopicCommand ]))

(def ^:private string-serializer
  (proxy [ZkSerializer] []
    (serialize [data] (.getBytes data "UTF-8"))
    (deserialize [bytes] (when bytes (String. bytes "UTF-8")))))


(defn zookeeper-client [{:keys [zookeeper-port        zookeeper-host]
                       :or   {zookeeper-port "2181" zookeeper-host "127.0.0.1"}}]
  (ZkClient. (str zookeeper-host ":" zookeeper-port) 500 500 string-serializer))

(defn create-topic!
  [zk-client topic & {:keys [partitions replicas]
                      :or   {partitions 1 replicas 1}}]
  (CreateTopicCommand/createTopic zk-client topic partitions replicas "")
  topic)

(defn create-topic [& args]
  (try (apply create-topic! args)
    (catch TopicExistsException e nil)))
