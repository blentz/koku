FROM centos/python-36-centos7

EXPOSE 9187

ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    SUMMARY="prometheus-postgresql-exporter" \
    DESCRIPTION="prometheus-postgresql-exporter"

LABEL summary="$SUMMARY" \
      description="$DESCRIPTION" \
      io.k8s.description="$DESCRIPTION" \
      io.k8s.display-name="prometheus-postgresql-exporter" \
      io.openshift.expose-services="9187:tcp" \
      io.openshift.tags="prometheus,monitoring,postgresql" \
      com.redhat.component="prometheus" \
      name="prometheus-postgresql-exporter" \
      version="1" \
      maintainer="Red Hat Subscription Management Services"

# build/install here
WORKDIR /opt/app-root/src
COPY postgresql_exporter .
RUN pip install --upgrade pip; \
    pip install -r requirements.txt

CMD ["/opt/app-root/src/postgresql_exporter.py"]
