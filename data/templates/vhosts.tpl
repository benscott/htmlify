<VirtualHost *:443>
    ServerAdmin webmaster@localhost
    ServerName {{DOMAIN}}
    DocumentRoot {{DOCUMENT_ROOT}}
    ErrorLog ${APACHE_LOG_DIR}/error.log
    CustomLog ${APACHE_LOG_DIR}/access.log combined

    SSLEngine on
    SSLCertificateFile /etc/ssl/certs/combined.crt
    SSLCertificateKeyFile /etc/ssl/private/server.key

    <Directory />
            Options FollowSymLinks
            AllowOverride None
    </Directory>
    <Directory {{DOCUMENT_ROOT}}/>
            Options Indexes FollowSymLinks MultiViews
            AllowOverride All
            Order allow,deny
            allow from all
    </Directory>

</VirtualHost>