(ns taoensso.carmine.tests.commands.show
  (:require [clojure.string :as str]
            [clojure.test     :as test :refer :all]
            [taoensso.carmine.tests.helpers :as helpers]
            [taoensso.carmine :as car  :refer (wcar)]))

(use-fixtures :each helpers/setup-teardown-db!)

(deftest show
  (testing "show"
    (let [id (helpers/dq1 (car/addjob "testq" "job1" 5000))
          job-info (helpers/dq1 (car/show id))] 
      (is (vector? job-info) "returns a vector")
      (is (= id (get (apply hash-map job-info) "id")) "returns the id of the job")
      (is (= "testq" (get (apply hash-map job-info) "queue")) "returns the queue of the job")
      (is (= "queued" (get (apply hash-map job-info) "state")) "returns the state of the job"))))
