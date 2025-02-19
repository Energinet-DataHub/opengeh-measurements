ARG UBUNTU_VERSION=24.04

# Base image
FROM mcr.microsoft.com/devcontainers/base:ubuntu-${UBUNTU_VERSION}

ARG ASDF_VERSION=0.15.0

# Install System Dependencies
RUN apt-get update \
  && apt-get install -y \
  git \
  curl \
  build-essential \
  # gnupg2 allows you to sign git commits with GPG
  gnupg2 \
  gcc \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Change to non-root user
ENV USER=vscode
ENV HOME=/home/$USER

# Install asdf
RUN git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v${ASDF_VERSION} --depth 1
RUN echo ". $HOME/.asdf/asdf.sh" >> $HOME/.bashrc \
  && echo ". $HOME/.asdf/completions/asdf.bash" >> $HOME/.bashrc \
  && echo ". $HOME/.asdf/asdf.sh" >> $HOME/.zshrc \
  && chown -R $USER:$USER $HOME/.asdf

# Add tool-versions and startup script
COPY --chown=${USER}:${USER} .tool-versions $HOME/.tool-versions
COPY --chown=${USER}:${USER} .devcontainer/scripts $HOME/scripts
RUN chmod +x $HOME/scripts/*

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Persist command history
RUN mkdir /commandhistory \
  && touch /commandhistory/.bash_history \
  && touch /commandhistory/.zsh_history \
  && chown -R $USER /commandhistory \
  && sed -i "1iexport PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" $HOME/.bashrc \
  && sed -i "1iexport PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.zsh_history" $HOME/.zshrc

# Install zsh-autosuggestions
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ~/.zsh/zsh-autosuggestions \
  && echo "source ~/.zsh/zsh-autosuggestions/zsh-autosuggestions.zsh" >> $HOME/.zshrc
