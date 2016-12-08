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
  ((actors :accessor get-actors
           :initform (make-hash-table :test 'equal))
   (lock :accessor lock-of
         :initform (make-recursive-lock "system-lock")))
  (:documentation
     "Holds the actors instances, creates actor refs
     and takes care of delivering the messages to the right actor"))

(defun make-system ()
  "Creates the actor system"
  (make-instance 'actor-system))

(defclass actor ()
  ((queue :accessor queue-of
          :initform '())
   (lock :initform (make-recursive-lock "queue-lock")
         :accessor lock-of)
   (scheduler :reader scheduler-of
              :initform nil
              :initarg :scheduler)
   (body :reader body-of
         :initarg :body)))

(defun make-actor (scheduler body)
  (make-instance 'actor :scheduler scheduler :body body))

(defclass actor-ref ()
  ((path :reader get-path
         :initarg :path))
  (:documentation
   "Reference to the real actor through path"))

(defun make-ref (path)
  "Create the actor ref with the given path"
  (make-instance 'actor-ref :path path))

(defclass scheduler () ())
(defclass pool-scheduler (scheduler)
  ((active :reader get-active
           :initform (make-hash-table :test 'equal))
   (pool-size :reader get-pool-size
              :initarg :pool-size)
   (pool :reader get-pool
         :initform nil)
   (lock :reader lock-of
         :initform (make-recursive-lock "pool-scheduler-lock"))))

(defun make-pool-scheduler (pool-size)
  (make-instance 'pool-scheduler :pool-size pool-size))

;; (defmethod initialize-instance :around ((s pool-scheduler) &key)
;;   )

(defgeneric insert-actor (system path actor))
(defgeneric get-actor (system ref))
(defgeneric actor-of (system actor &key name))
(defgeneric send (system ref message))
(defgeneric schedule (system scheduler path actor))
(defgeneric run (actor))

(defmethod insert-actor ((system actor-system) path (actor actor))
  "Inserts a ref into the system throwing if it already exists"
  (with-lock-held ((lock-of system))
    (let* ((actors (get-actors system))
           (key (gethash path actors)))
      (if key
          (error 'path-already-exists :text (format nil "path ~A already esists" path))
          (setf (gethash path actors) actor)))))

(defmethod get-actor ((system actor-system) (ref actor-ref))
  (with-lock-held ((lock-of system))
    (let* ((actors (get-actors system))
           (actor (gethash (get-path ref) actors)))
      (unless actor
        (error 'actor-doesnt-exists))
      actor)))

(defun random-string (length)
  (let ((chars "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"))
    (coerce (loop repeat length collect (aref chars (random (length chars))))
            'string)))

(defun make-path (base name)
  "Create a path from a base path and a name"
  (unless name
    (setf name (random-string 10)))
  (if base
      (format nil "~A/~A" base name)
      (format nil "/~A" name)))

(defmethod run ((actor actor))
  (labels ((pop-queue ()
             (with-lock-held ((lock-of actor))
               (let ((queue (queue-of actor)))
                 (when queue
                   (let ((head (car queue)))
                     (setf (queue-of actor) (cdr queue))
                     head)))))
           (queue-has-elem? ()
             (with-lock-held ((lock-of actor))
               (if (queue-of actor) t nil))))
    (let ((body (body-of actor))
          (message (pop-queue)))
      (when message
        (funcall body message)
        (when (queue-has-elem?)
          (run actor))))))

(defmethod schedule ((system actor-system) (scheduler pool-scheduler) path (actor actor))
  (with-lock-held ((lock-of scheduler))
    (let* ((active (get-active scheduler))
           (actor (gethash path active)))
      (unless actor
        (make-thread
         (lambda ()
           (run actor)
           (with-lock-held ((lock-of scheduler))
             (setf (gethash path active) nil))))))))

(defmethod send ((system actor-system) (ref actor-ref) message)
  (let* ((actor (get-actor system ref))
         (queue (queue-of actor))
         (path (get-path ref)))
    (with-lock-held ((lock-of actor))
      (setf (queue-of actor) (append queue (list message))))
    (schedule system (scheduler-of actor) path actor)))

(defmethod actor-of ((system actor-system) (actor actor) &key name)
  "Create an actor ref for the actor passed and inserts it into the system"
  (alog.debug "creating actor ~a" name)
  (let* ((path (make-path nil name))
         (ref (make-ref path)))
     (insert-actor system path actor)
     ref))

(defparameter *system* (make-system))
(defparameter *pool* (make-pool-scheduler 10))

(defparameter *actor1* (make-actor *pool* (lambda (m) (format t "Actor1: ~A" m))))
(defparameter *actor2* (make-actor *pool* (lambda (m) (format t "Actor2: ~A" m))))

(defun test ()
  (let ((ref1 (actor-of *system* *actor1*))
        (ref2 (actor-of *system* *actor2*)))
    (send *system* ref1 "prova")))
