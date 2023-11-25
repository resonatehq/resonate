#!/usr/bin/env bash
set -euo pipefail

## Check that the script is not being run on Windows. 

if [[ ${OS:-} = Windows_NT ]]; then
    echo 'error: Please install resonate from source or using Windows Subsystem for Linux'
    exit 1
fi

## Setup some color variables for printing colored output. 

# Reset
Color_Off=''

# Regular Colors
Red=''
Green=''
Dim='' 

# Bold
Bold_White=''
Bold_Green=''

if [[ -t 1 ]]; then
    # Reset
    Color_Off='\033[0m' # Text Reset

    # Regular Colors
    Red='\033[0;31m'   
    Green='\033[0;32m' 
    Dim='\033[0;2m'    

    # Bold
    Bold_Green='\033[1;32m' 
    Bold_White='\033[1m'    
fi

error() {
    echo -e "${Red}error${Color_Off}:" "$@" >&2
    exit 1
}

info() {
    echo -e "${Dim}$@ ${Color_Off}"
}

info_bold() {
    echo -e "${Bold_White}$@ ${Color_Off}"
}

success() {
    echo -e "${Green}$@ ${Color_Off}"
}

## Determine the target. 

# Determine the target platform. 
UNAME_OUT="$(uname -s)"
case "${UNAME_OUT}" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=darwin;;
    *)          "Unsupported platform" >&2; exit 1
esac

# Determine the target architecture.
UNAME_M="$(uname -m)"
case "${UNAME_M}" in
    x86_64)        ARCH=x86_64;;
    arm64|aarch64) ARCH=aarch64 ;;
    *)             "Unsupported architecture" >&2; exit 1   
esac

## Setup resonate. 

# Construct the bucket URL to download the binary from.
bucket_url="https://storage.googleapis.com/resonate-release/${PLATFORM}-${ARCH}/resonate"

# Create the bin directory if it doesn't exist already.
install_dir=$HOME/.resonate
bin_dir=$install_dir/bin
exe=resonate

if [[ ! -d $bin_dir ]]; then
  mkdir -p "$bin_dir" || error "Failed to create install directory $bin_dir"
fi

exe_path=$bin_dir/$exe

# Download the binary into the temporary directory. cl
info "Downloading resonate from $bucket_url..."
curl --fail --location --progress-bar --output $exe_path $bucket_url ||
    error "Failed to download resonate from \"$bucket_url\""

# Make the binary executable. 
chmod +x $exe_path ||
    error 'Failed to set permissions on resonate executable'


# Print a success message. 
tildify() {
    if [[ $1 = $HOME/* ]]; then
        local replacement=\~/

        echo "${1/$HOME\//$replacement}"
    else
        echo "$1"
    fi
}

success "Resonate was installed successfully to $Bold_Green$(tildify "$exe_path")."

if command -v resonate >/dev/null; then
  # Resonate already installed.
  echo "Run 'resonate --help' to get started"
  exit
fi

refresh_command=''
tilde_bin_dir=$(tildify "$exe_path")

echo

case $(basename "$SHELL") in
  fish )

    commands=(
        "set --export PATH $bin_dir \$PATH"
    )

    fish_config=$HOME/.config/fish/config.fish
    tilde_fish_config=$(tildify "$fish_config")

    if [[ -w $fish_config ]]; then
        {
            echo -e '\n# resonate'

            for command in "${commands[@]}"; do
                echo "$command"
            done
        } >>"$fish_config"

        info "Added \"$tilde_bin_dir\" to \$PATH in \"$tilde_fish_config\""

        refresh_command="source $tilde_fish_config"
    else
        echo "Manually add the directory to $tilde_fish_config (or similar):"

        for command in "${commands[@]}"; do
            info_bold "  $command"
        done
    fi
    ;;

  zsh )

    # Add directory to PATH in zsh 
    commands=(
        "export PATH=\"$bin_dir:\$PATH\""
    )

    zsh_config=$HOME/.zshrc
    tilde_zsh_config=$(tildify "$zsh_config")

    if [[ -w $zsh_config ]]; then
        {
            echo -e '\n# resonate'

            for command in "${commands[@]}"; do
                echo "$command"
            done
        } >>"$zsh_config"

        info "Added \"$tilde_bin_dir\" to \$PATH in \"$tilde_zsh_config\""

        refresh_command="exec $SHELL"
    else
        echo "Manually add the directory to $tilde_zsh_config (or similar):"

        for command in "${commands[@]}"; do
            info_bold "  $command"
        done
    fi
    ;;

  bash )  

    commands=(
        "export PATH=$bin_dir:\$PATH"
    )

    bash_configs=(
        "$HOME/.bashrc"
        "$HOME/.bash_profile"
    )

    if [[ ${XDG_CONFIG_HOME:-} ]]; then
        bash_configs+=(
            "$XDG_CONFIG_HOME/.bash_profile"
            "$XDG_CONFIG_HOME/.bashrc"
            "$XDG_CONFIG_HOME/bash_profile"
            "$XDG_CONFIG_HOME/bashrc"
        )
    fi

    set_manually=true
    for bash_config in "${bash_configs[@]}"; do
        tilde_bash_config=$(tildify "$bash_config")

        if [[ -w $bash_config ]]; then
            {
                echo -e '\n# resonate'

                for command in "${commands[@]}"; do
                    echo "$command"
                done
            } >>"$bash_config"

            info "Added \"$tilde_bin_dir\" to \$PATH in \"$tilde_bash_config\""

            refresh_command="source $bash_config"
            set_manually=false
            break
        fi
    done

    if [[ $set_manually = true ]]; then
        echo "Manually add the directory to $tilde_bash_config (or similar):"

        for command in "${commands[@]}"; do
            info_bold "  $command"
        done
    fi
    ;;

  * )
    echo 'Manually add the directory to ~/.bashrc (or similar):'
    info_bold "  export PATH=\"$bin_dir:\$PATH\""
    ;;
esac

echo
info "To get started, run:"
echo

if [[ $refresh_command ]]; then
    info_bold "  $refresh_command"
fi

info_bold "  resonate --help"