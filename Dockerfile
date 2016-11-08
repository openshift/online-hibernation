# Free Tier Force Sleep controller 

FROM rhel7.2:7.2-released

ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH=/go

LABEL BZComponent="oso-force-sleep"
LABEL Name="openshift3/oso-force-sleep"
LABEL Version="v3.3.0.0"
LABEL Architecture="x86_64"

ADD . /go/src/github.com/openshift/online/force-sleep

RUN yum-config-manager --enable rhel-7-server-optional-rpms && \
    INSTALL_PKGS="golang make" && \
    yum install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum clean all -y

WORKDIR /go/src/github.com/openshift/online/force-sleep
RUN export GOPATH && make install test && cp /go/bin/force-sleep /usr/bin/force-sleep
ENTRYPOINT ["/usr/bin/force-sleep"]
