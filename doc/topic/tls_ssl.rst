SSL and TLS
===========


For detailed howtos, see the `openssl HOWTO's <http://www.openssl.org/docs/HOWTO/>`_.


Generate a private key
------------------------

The openssl toolkit can be used to generate an RSA Private Key and CSR (Certificate Signing Request). It can also be
used to generate self-signed certificates which can be used in controlled environments.

The first step is to create your RSA Private Key. This key is a 1024 bit RSA key which is encrypted using
Triple-DES and stored in a PEM format so that it is readable as ASCII text. A RSA key can be used both for encryption
and for signing.

.. code-block:: bash

    $ openssl genrsa -out server.key 1024
    Generating RSA private key, 1024 bit long modulus
    ....++++++
    ....................++++++
    e is 65537 (0x10001)


Generate a CSR (Certificate Signing Request)
--------------------------------------------

To create a certificate, you need to start with a certificate
request (or, as some certificate authorities like to put
it, "certificate signing request", since that's exactly what they do,
they sign it and give you the result back, thus making it authentic
according to their policies).  A certificate request can then be sent
to a certificate authority to get it signed into a certificate, or if
you have your own certificate authority, you may sign it yourself, or
if you need a self-signed certificate (because you just want a test
certificate or because you are setting up your own CA).


During the generation of the CSR, you will be prompted for the X.509 attributes of the certificate. The command to generate the CSR is as follows:

.. code-block:: bash

    $ openssl req -new -key server.key -out server.csr
    You are about to be asked to enter information that will be incorporated
    into your certificate request.
    What you are about to enter is what is called a Distinguished Name or a DN.
    There are quite a few fields but you can leave some blank
    For some fields there will be a default value,
    If you enter '.', the field will be left blank.
    -----
    Country Name (2 letter code) [AU]:NO
    State or Province Name (full name) [Some-State]:
    Locality Name (eg, city) []:
    Organization Name (eg, company) [Internet Widgits Pty Ltd]:Piped Example
    Organizational Unit Name (eg, section) []:
    Common Name (eg, YOUR name) []:piped.localhost
    Email Address []:

    Please enter the following 'extra' attributes
    to be sent with your certificate request
    A challenge password []:
    An optional company name []:


Generating a Self-Signed Certificate
------------------------------------

If you don't want to deal with another certificate authority, or just
want to create a test certificate for yourself.  This is similar to
creating a certificate request, but creates a certificate instead of
a certificate request.  This is NOT the recommended way to create a
CA certificate, but may be sufficient in controlled environments.

To generate a temporary certificate which is good for 1095 days, issue the following command:

.. code-block:: bash

    $ openssl x509 -req -days 1095 -in server.csr -signkey server.key -out server.crt
    Signature ok
    subject=/C=NO/ST=Some-State/O=Piped Example/CN=piped.localhost
    Getting Private key