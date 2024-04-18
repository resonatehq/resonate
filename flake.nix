# This flake was initially generated by fh, the CLI for FlakeHub (version 0.1.10)
{
  # A helpful description of the flake
  description = "Resonate: a dead simple programming model for modern applications";

  # Flake inputs
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/*";
    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-schemas.url = "https://flakehub.com/f/DeterminateSystems/flake-schemas/*";
  };

  # Flake outputs that other flakes can use
  outputs = { self, nixpkgs, gomod2nix, flake-schemas }:
    let
      # Version inference
      lastModifiedDate = self.lastModifiedDate or self.lastModified or "19700101";
      version = "${builtins.substring 0 8 lastModifiedDate}-${self.shortRev or "dirty"}";

      # Helpers for producing system-specific outputs
      supportedSystems = [ "x86_64-linux" "aarch64-darwin" "x86_64-darwin" "aarch64-linux" ];
      forEachSupportedSystem = f: nixpkgs.lib.genAttrs supportedSystems (system: f {
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlays.default ];
        };
        inherit system;
      });
    in
    {
      # Schemas tell Nix about the structure of your flake's outputs
      inherit (flake-schemas) schemas;

      # Custom attributes for Nixpkgs
      overlays.default = final: prev: {
        buildGoApplication = gomod2nix.legacyPackages.${prev.stdenv.system}.buildGoApplication;
        gomod2nixPkg = gomod2nix.packages.${prev.stdenv.system}.default;
      };

      # Development environments
      devShells = forEachSupportedSystem ({ pkgs, system }: {
        default = pkgs.mkShell {
          # Pinned packages available in the environment
          packages = with pkgs; [
            # Go
            go_1_21
            gotools # goimports, godoc, etc.
            golangci-lint # Go linter

            # Nix + Go dependency management
            gomod2nixPkg

            # Tool for generating mocks
            mockgen

            # protoc
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc

            # OpenAPI generator
            oapi-codegen

            # Nix formatter
            nixpkgs-fmt
          ]
          # Broken on aarch64-linux
          ++ pkgs.lib.optional (system != "aarch64-linux") (with pkgs; [ semgrep ]);
        };
      });

      # Package outputs
      packages = forEachSupportedSystem ({ pkgs, ... }: rec {
        # The Resonate server
        resonate = pkgs.buildGoApplication rec {
          pname = "resonate";
          inherit version;
          src = self;
          modules = ./gomod2nix.toml;

          # Required for SQLite on Linux
          CGO_ENABLED = 1;

          # Make the binary static on Linux
          ldflags = [
            "-s"
            "-w"
          ] ++ pkgs.lib.optional (pkgs.stdenv.isLinux) [
            "-extldflags=-static"
            "-linkmode=external"
          ];

          # Use glibc on Linux
          buildInputs = pkgs.lib.optional
            (pkgs.stdenv.isLinux)
            (with pkgs; [ glibc glibc.static ]);

          # Provides the `installShellCompletion` shell function
          nativeBuildInputs = with pkgs; [ installShellFiles ];

          # Provides shell completion for bash, zsh, and fish
          postInstall = ''
            installShellCompletion --cmd ${pname} \
              --bash <($out/bin/${pname} completion bash) \
              --zsh <($out/bin/${pname} completion zsh) \
              --fish <($out/bin/${pname} completion fish)
          '';
        };

        # Test harness (TODO: make this a flake as well)
        durable-promise-test-harness = pkgs.buildGo121Module rec {
          name = "durable-promise-test-harness";
          src = pkgs.fetchFromGitHub {
            owner = "resonatehq";
            repo = name;
            rev = "43a2b602ca1ed5a019f0e9341efdab3484b3e2e0";
            hash = "sha256-9IfrHQ+8CB/yLHtmZwcajvQ2yWrqZJi2frS+wBRsGfY=";
          };
          vendorHash = "sha256-n15ECdUjvwg8H0uVZzP40E9vpNSJrkvqxQWBTGkqcs8=";
        };

        # This enables you to use the shorthand `nix build` to build the server
        default = resonate;
      });

      # Docker image outputs
      dockerImages = forEachSupportedSystem ({ pkgs, ... }: rec {
        # The Resonate server as an image
        resonate =
          let
            # A version of Nixpkgs solely for x86_64 Linux (the built image's system)
            linuxPkgs = pkgs.legacyPackages.x86_64-linux;
          in
          pkgs.dockerTools.buildLayeredImage {
            name = "resonate-${version}";

            # Extra packages for the image
            contents = with linuxPkgs.dockerTools; [
              # Standard requirement for HTTP and the like
              caCertificates
            ];

            config = {
              # The image ENTRYPOINT (Nix automatically builds this path)
              Entrypoint = [ "${self.packages.x86_64-linux.default}/bin/resonate" ];
              # EXPOSE statements for ports
              ExposedPorts = {
                "8001" = { };
                "50051" = { };
              };
            };
          };
      });
    };
}
