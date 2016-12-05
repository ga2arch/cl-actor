;;;; cl-actor.lisp

(in-package #:cl-actor)
(a-cl-logger:define-logger alog ())

;;; "cl-actor" goes here. Hacks and glory await!

(defclass actor-system ()
  ((actors :reader get-actors
           :initform (make-hash-table)))
  (:documentation
     "Holds the actors instances, creates actor refs
     and takes care of delivering the messages to the right actor"))

(defun make-system ()
  "Creates the actor system"
  (make-instance 'actor-system))

(defclass actor () ())
(defclass actor-ref ()
  ((path :reader get-path
         :initarg :path))
  (:documentation
   "Reference to the real actor through path"))

(defun make-ref (path)
  "Create the actor ref with the given path"
  (make-instance 'actor-ref :path path))

(defgeneric actor-of (system actor name))
(defmethod actor-ref ((system actor-system) (actor actor) name)
  (alog.debug "creating actor ~a" name))
