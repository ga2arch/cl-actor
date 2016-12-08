;;;; cl-actor.asd

(asdf:defsystem #:cl-actor
  :description "Akka like actor system"
  :author "Gabriele Carrettoni <gabriele.carrettoni@gmail.com>"
  :license "MIT"
  :depends-on (#:alexandria
               #:bordeaux-threads
               #:a-cl-logger)
  :serial t
  :components ((:file "package")
               (:file "cl-actor")))
