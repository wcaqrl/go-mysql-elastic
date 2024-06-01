FROM centos:7

WORKDIR /opt/www

COPY . /opt/www

RUN set -ex \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && chmod +x /opt/www/go-mysql-elasticsearch

ENTRYPOINT ["/opt/www/go-mysql-elasticsearch", "-config=app.ini", "-job"]
