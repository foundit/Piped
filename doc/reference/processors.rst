Available processors
====================

.. automodule:: piped.processors




Utility processors
------------------

.. currentmodule:: piped.processors.util_processors


.. _callback-deferred:

callback-deferred
^^^^^^^^^^^^^^^^^

.. autoclass:: CallbackDeferred


.. _clean-baton:

clean-baton
^^^^^^^^^^^
.. autoclass:: BatonCleaner


.. _collect-batons:

collect-batons
^^^^^^^^^^^^^^
.. autoclass:: BatonCollector


.. _decode-string:

decode-string
^^^^^^^^^^^^^^
.. autoclass:: StringDecoder


.. _format-string:

format-string
^^^^^^^^^^^^^
.. autoclass:: StringFormatter


.. _encode-string:

encode-string
^^^^^^^^^^^^^^
.. autoclass:: StringEncoder


.. _eval-lambda:

eval-lambda
^^^^^^^^^^^^^^
.. autoclass:: LambdaProcessor


.. _exec-code:

exec-code
^^^^^^^^^^^^^^

.. autoclass:: ExecProcessor


.. _flatten-list-of-dictionaries:

flatten-list-of-dictionaries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: FlattenDictionaryList


.. _flatten-nested-lists:

flatten-nested-lists
^^^^^^^^^^^^^^^^^^^^
.. autoclass:: NestedListFlattener


.. _for-each:

for-each
^^^^^^^^
.. autoclass:: ForEach


.. _group-by-value:

group-by-value
^^^^^^^^^^^^^^
.. autoclass:: DictGrouper


.. _increment-counter:

increment-counter
^^^^^^^^^^^^^^^^^
.. autoclass:: CounterIncrementer


.. _lambda-decider:

lambda-decider
^^^^^^^^^^^^^^
.. autoclass:: LambdaConditional


.. _log:

log
^^^
.. autoclass:: Logger


.. _merge-with-dict:

merge-with-dict
^^^^^^^^^^^^^^^
.. autoclass:: MergeWithDictProcessor


.. _passthrough:

passthrough
^^^^^^^^^^^^^^
.. autoclass:: Passthrough


.. _pretty-print:

pretty-print
^^^^^^^^^^^^^^
.. autoclass:: PrettyPrint


.. _print-failure-traceback:

print-failure-traceback
^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: PrintTraceback


.. _print-nth:

print-nth
^^^^^^^^^^^^^^
.. autoclass:: NthPrinter


.. _raise-exception:

raise-exception
^^^^^^^^^^^^^^^
.. autoclass:: RaiseException


.. _remap:

remap
^^^^^^^^^^^^^^^
.. autoclass:: RemapProcessor


.. _set-value:

set-value
^^^^^^^^^^^^^^^
.. autoclass:: ValueSetter


.. _set-values:

set-values
^^^^^^^^^^^^^^^
.. autoclass:: MappingSetter


.. _shutdown:

shutdown
^^^^^^^^
..autoclass:: Shutdown


.. _stop:

stop
^^^^^^^^^^^^^^
.. autoclass:: Stopper


.. _wait:

wait
^^^^^^^^^^^^^^
.. autoclass:: Waiter


.. _wrap-coroutine:

wrap-coroutine
^^^^^^^^^^^^^^
.. autoclass:: CoroutineWrapper



Datetime processors
-------------------

.. currentmodule:: piped.processors.datetime_processors


.. _format-date:

format-date
^^^^^^^^^^^^^^
.. autoclass:: DateFormatter


.. _parse-date:

parse-date
^^^^^^^^^^^^^^
.. autoclass:: DateTimeParser





File processors
---------------

.. currentmodule:: piped.processors.file_processors


.. _append-to-file:

append-to-file
^^^^^^^^^^^^^^
.. autoclass:: FileAppender


.. _append-to-log:

append-to-log
^^^^^^^^^^^^^^
.. autoclass:: LogAppender



JSON processors
---------------

.. currentmodule:: piped.processors.json_processors


.. _decode-json:

decode-json
^^^^^^^^^^^^^^
.. autoclass:: JsonDecoder


.. _encode-json:

encode-json
^^^^^^^^^^^^^^
.. autoclass:: JsonEncoder



Perspective broker processors
-----------------------------

.. currentmodule:: piped.processors.spread_processors

.. automodule:: piped.processors.spread_processors

.. _pb-call-remote:

pb-call-remote
^^^^^^^^^^^^^^
.. autoclass:: CallRemote



Pipeline processors
-------------------

.. currentmodule:: piped.processors.pipeline_processors


.. _diagram-dependencies:

diagram-dependencies
^^^^^^^^^^^^^^^^^^^^
.. autoclass:: DependencyDiagrammer

.. _diagram-pipelines:

diagram-pipelines
^^^^^^^^^^^^^^^^^
.. autoclass:: PipelineDiagrammer


.. _run-pipeline:

run-pipeline
^^^^^^^^^^^^^^
.. autoclass:: PipelineRunner


.. _scatter-gather:

scatter-gather
^^^^^^^^^^^^^^
.. autoclass:: ScatterGatherer



Process processors
------------------

.. currentmodule:: piped.processors.process_processors


.. _parse-iostat-output:

parse-iostat-output
^^^^^^^^^^^^^^^^^^^
.. autoclass:: IOStatParser


.. _render-dot:

render-dot
^^^^^^^^^^^^^^
.. autoclass:: RenderDot



SMTP processors
---------------

.. currentmodule:: piped.processors.smtp_processors


.. _create-email-message:

create-email-message
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: CreateEmailMessage


.. _replace-email-headers:

replace-email-headers
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SetMessageHeaders


.. _send-email:

send-email
^^^^^^^^^^

.. autoclass:: SendEmail



Tick processors
---------------

.. currentmodule:: piped.processors.tick_processors

.. automodule:: piped.processors.tick_processors


.. _start-tick-interval:

start-tick-interval
^^^^^^^^^^^^^^^^^^^
.. autoclass:: StartInterval


.. _stop-tick-interval:

stop-tick-interval
^^^^^^^^^^^^^^^^^^
.. autoclass:: StopInterval



Web processors
--------------

.. currentmodule:: piped.processors.web_processors


.. _determine-ip:

determine-ip
^^^^^^^^^^^^^^
.. autoclass:: IPDeterminer


.. _set-http-expires:

set-http-expires
^^^^^^^^^^^^^^^^
.. autoclass:: SetExpireHeader


.. _set-http-headers:

set-http-headers
^^^^^^^^^^^^^^^^
.. autoclass:: SetHttpHeaders


.. _write-web-response:

write-web-response
^^^^^^^^^^^^^^^^^^
.. autoclass:: ResponseWriter



XML processors
--------------

.. currentmodule:: piped.processors.xml_processors


.. _remove-markup:

remove-markup
^^^^^^^^^^^^^
.. autoclass:: MarkupRemover



Contrib processors
------------------



Status testing processors
^^^^^^^^^^^^^^^^^^^^^^^^^

.. currentmodule:: piped.contrib.status_testing.processors


.. _create-statustest-reporter:

create-statustest-reporter
""""""""""""""""""""""""""

.. autoclass:: ReporterCreator


.. _wait-for-statustest-reporter:

wait-for-statustest-reporter
""""""""""""""""""""""""""""

.. autoclass:: WaitForReporterProcessing



Validation processors
^^^^^^^^^^^^^^^^^^^^^


.. _validate-with-formencode:

validate-with-formencode
""""""""""""""""""""""""
.. autoclass:: piped.contrib.validation.processors.FormEncodeValidator




ZMQ processors
^^^^^^^^^^^^^^


.. _parse-as-mongrel-request:

parse-as-mongrel-request
""""""""""""""""""""""""
.. autoclass:: piped.contrib.zmq.mongrel2_processors.MongrelRequestToBatonParser


.. _send-zmq-message:

send-zmq-message
""""""""""""""""""""""""
.. autoclass:: piped.contrib.zmq.processors.MessageSender


.. _send-mongrel-reply:

send-mongrel-reply
""""""""""""""""""""""""
.. autoclass:: piped.contrib.zmq.mongrel2_processors.MongrelReplySender