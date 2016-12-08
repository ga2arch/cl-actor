;;;; cl-actor.lisp

(in-package #:cl-actor)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (a-cl-logger:define-logger alog ())
  (use-package :bordeaux-threads))

;;; "cl-actor" goes here. Hacks and glory await!

;;; Exceptions
(define-condition path-already-exists (error)
  ((text :initarg :text :reader text)))

(define-condition actor-doesnt-exists (error)
  ((text :reader text :initform "actor doesn't exists")))
;;;

(defclass actor-system ()
  ((actors :reader get-actors
           :initform (make-hash-table)))
  (:documentation
     "Holds the actors instances, creates actor refs
     and takes care of delivering the messages to the right actor"))

(defun make-system ()
  "Creates the actor system"
  (make-instance 'actor-system))

(defclass actor ()
  ((queue :reader queue-of
          :initform '())
   (lock :initform (make-recursive-lock "queue-lock")
         :accessor lock-of)
   (scheduler :reader scheduler
              :initform nil)))

(defun make-actor ()
  (make-instance 'actor))

(defclass actor-ref ()
  ((path :reader get-path
         :initarg :path))
  (:documentation
   "Reference to the real actor through path"))

(defun make-ref (path)
  "Create the actor ref with the given path"
  (make-instance 'actor-ref :path path))

(defgeneric insert-ref (system ref))
(defgeneric get-actor (system ref))
(defgeneric actor-of (system actor name))
(defgeneric send (system ref message))

(defmethod insert-ref ((system actor-system) (ref actor-ref))
  "Inserts a ref into the system throwing if it already exists"
  (let* ((actors (get-actors system))
         (hash (gethash ref actors)))
    (if hash
        (error 'path-already-exists :text (format nil "path ~A already esists" (get-path ref)))
        (setf hash ref))))

(defmethod get-actor ((system actor-system) (ref actor-ref))
  (let* ((actors (get-actors system))
         (hash (gethash ref actors)))
    (unless hash
      (error 'actor-doesnt-exists))
    hash))

(defun make-path (base name)
  "Create a path from a base path and a name"
  (if base
      (format nil "~A/~A" base name)
      (format nil "/~A" name)))

(defmethod send ((system actor-system) (ref actor-ref) message)
  (let* ((actor (get-actor system ref))
         (queue (queue-of actor)))
    (with-lock-held ((lock-of actor))
      (setf queue (append queue (list message))))
    (schedule actor)))

(defmethod actor-of ((system actor-system) (actor actor) name)
  "Create an actor ref for the actor passed and inserts it into the system"
  (alog.debug "creating actor ~a" name)
  (let* ((path (make-path nil name))
          (ref (make-ref path)))
     (insert-ref path ref)
     ref))
