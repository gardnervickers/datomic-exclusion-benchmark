(ns datomic-exclusion-benchmark.core
  (:require [datomic.api :as d]
            [criterium.core :refer [bench quick-bench with-progress-reporting]]))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn get-pending-segments-pull
  "Returns any :pending-event entities that are not complete
  and hav"
  [db]
  (d/q '[:find [(pull ?e [*]) ...]
         :in $
         :where
         [?e :pending-event/complete? false]
         [?e :pending-event/time ?time]
         [(.after (java.util.Date.) ?time)]]
       db))

(defn get-pending-segments
  "Returns any :pending-event entities that are not complete
  and hav"
  [db]
  (d/q '[:find [?e ...]
         :in $
         :where
         [?e :pending-event/complete? false]
         [?e :pending-event/time ?time]
         [(.after (java.util.Date.) ?time)]]
       db))

(defn exclude-eids [eid exclude-list]
  ((comp not (set exclude-list)) eid))

(defn get-pending-segments-predicate
  "Returns any :pending-event entities that are not complete
  and hav"
  [db exclude-eids]
  (d/q '[:find [?e ...]
         :in $ ?exclude-eids
         :where
         [?e :pending-event/complete? false]
         [?e :pending-event/time ?time]
         [(.after (java.util.Date.) ?time)]
         [(datomic-exclusion-benchmark.core/exclude-eids ?e ?exclude-eids)]]
       db
       exclude-eids))

(defn setup-datomic! [db-uri]
  (let [schema [{:db/id #db/id [:db.part/db]
                 :db/ident :pending-event/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}

                {:db/id #db/id [:db.part/db]
                 :db/ident :pending-event/time
                 :db/valueType :db.type/instant
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}

                {:db/id #db/id [:db.part/db]
                 :db/ident :pending-event/complete?
                 :db/valueType :db.type/boolean
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}

                {:db/id #db/id [:db.part/db]
                 :db/ident :pending-event/uuid
                 :db/valueType :db.type/uuid
                 :db/unique :db.unique/identity
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}]

        test-data (vec (pmap (fn [x]
                               {:db/id (d/tempid :db.part/user)
                                :pending-event/name (str (java.util.UUID/randomUUID))
                                :pending-event/time (new java.util.Date (* 1000 (rand-int 100000)))
                                :pending-event/uuid (java.util.UUID/randomUUID)
                                :pending-event/complete? (rand-nth [true false false false])})
                             (range 1 10000)))
        _ (d/create-database db-uri)
        conn (d/connect db-uri)]
    @(d/transact conn schema)
    @(d/transact conn test-data)))

(let [_ (d/delete-database "datomic:mem://cron")
      _ (setup-datomic! "datomic:mem://cron")
      conn (d/connect "datomic:mem://cron")
      db (d/db conn)
      eids-to-exclude (set (take 20 (get-pending-segments db)))]
  (println "Benchmarking using filtered db")
  (with-progress-reporting
    (quick-bench (let [filtered-db (d/filter db (fn [_ datom] ((comp not eids-to-exclude) (.e datom))))]
                   (get-pending-segments-pull filtered-db)) :verbose))
  (println "Benchmarking using filter-join")
  (with-progress-reporting
    (quick-bench (let [filtered-db (d/filter db (fn [_ datom] ((comp not eids-to-exclude) (.e datom))))
                       pending-segments (get-pending-segments db)]
                   (mapv #(d/pull filtered-db '[*] %) pending-segments)) :verbose))
  (println "Benchmarking using predicate")
  (with-progress-reporting
    (quick-bench (let [pending-segments (get-pending-segments db)]
                   (get-pending-segments-predicate db eids-to-exclude)) :verbose)))
