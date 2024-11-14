FROM continuumio/miniconda3:24.9.2-0

USER root

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000
ENV USERNAME=${USERNAME}
ENV USER_UID=${USER_UID}
ENV USER_GID=${USER_GID}

RUN apt-get update -q \
    && apt-get install -q -y --no-install-recommends \
        build-essential \
        ripgrep \
        pandoc \
        adduser \
        git \
        grep \
        curl \
        wget \
        vim

RUN conda install -y python=3.9 \
    && pip install --no-cache-dir pipx \
    && pipx reinstall-all
    
RUN conda install -c conda-forge gh jupyterlab

COPY requirements.txt /requirements.txt

RUN python3 -m pip install -r requirements.txt

RUN python3 -m pip install --upgrade flake8-pytest-importorskip click==8.0.4 black==24.4.2 flake8==4.0.1

RUN groupadd -g ${USER_GID} ${USERNAME} && \
    adduser --disabled-password --uid ${USER_UID} --gid ${USER_GID} --gecos "" ${USERNAME} && \
    echo "${USERNAME} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers
    
USER $USERNAME

# FROM mcr.microsoft.com/devcontainers/miniconda:1-3
# 
# # Copy environment.yml (if found) to a temp location so we update the environment. Also
# # copy "noop.txt" so the COPY instruction does not fail if no environment.yml exists.
# COPY environment.yml* .devcontainer/noop.txt /tmp/conda-tmp/
# RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/conda env update -n base -f /tmp/conda-tmp/environment.yml; fi \
#     && rm -rf /tmp/conda-tmp
# 
# COPY requirements.txt /requirements.txt
# 
# RUN conda install -y python=3.9 \
#     && pip install --no-cache-dir pipx \
#     && pipx reinstall-all
# 
# 
# # [Optional] Uncomment this section to install additional OS packages.
# # RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
# #     && apt-get -y install --no-install-recommends <your-package-list-here>
 