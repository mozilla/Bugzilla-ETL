FROM python:3.6.4

ARG BUILD_URL=
ARG REPO_CHECKOUT=
ARG REPO_URL=https://github.com/mozilla/Bugzilla-ETL.git
ARG HOME=/app
ARG USER=app

WORKDIR $HOME
RUN mkdir -p /etc/dpkg/dpkg.cfg.d \
    &&  echo "path-exclude=/usr/share/locale/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/man/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  echo "path-exclude=/usr/share/doc/*" >> /etc/dpkg/dpkg.cfg.d/excludes \
    &&  apt-get -qq update \
    &&  apt-get -y install --no-install-recommends \
        libffi-dev \
        libssl-dev \
        curl \
        git \
        build-essential \
        vim-tiny \
        nano \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc/* /usr/share/man/* /usr/share/locale/* \
    && git clone $REPO_URL $HOME \
    && git checkout $REPO_CHECKOUT \
    && python -m pip --no-cache-dir install --user -r requirements.txt \
    && export PYTHONPATH=.:vendor \
    && python resources/docker/version.py

RUN addgroup --gid 10001 $USER \
    && adduser \
      --gid 10001 \
      --uid 10001 \
      --home $HOME \
      --shell /usr/sbin/nologin \
      --no-create-home \
      --disabled-password \
      --gecos we,dont,care,yeah \
      $USER \
    && mkdir $HOME/logs \
    && chown -R $USER:$USER $HOME

USER $USER

CMD export PYTHONPATH=.:vendor \
    && python ./bugzilla_etl/bz_etl.py --settings=resources/docker/config.json
