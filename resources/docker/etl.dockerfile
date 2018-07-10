FROM python:3.6.4

ARG BRANCH=dev
ARG TAG=v0.2
ARG HOME=/app
ARG USER=app

WORKDIR $HOME
RUN mkdir -p /etc/dpkg/dpkg.cfg.d \
    &&  echo "path-exclude=/usr/share/locale/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/man/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/doc/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  apt-get -qq update \
    &&  apt-get -y install --no-install-recommends \
        build-essential \
        libffi-dev \
        libssl-dev \
        curl \
        git \
        vim-tiny \
        nano \
        sudo \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/* /usr/share/locale/* \
    && git clone https://github.com/mozilla/Bugzilla-ETL.git $HOME \
    && git checkout $BRANCH \
    && git config --global user.email "klahnakoski@mozilla.com" \
    && git config --global user.name "Kyle Lahnakoski" \
    && chmod a+x resources/docker/crontab \
    && chmod a+x resources/docker/etl.sh \
    && cp resources/docker/crontab /etc/cron.daily/$USER

RUN addgroup --gid 10001 $USER
RUN adduser \
      --gid 10001 \
      --uid 10001 \
      --home $HOME \
      --shell /usr/sbin/nologin \
      --no-create-home \
      --disabled-password \
      --gecos we,dont,care,yeah \
      $USER

RUN mkdir $HOME/logs
RUN chown -R $USER:$USER $HOME
USER $USER
RUN python -m pip --no-cache-dir install --user -r requirements.txt
