# This flake was initially generated by fh, the CLI for FlakeHub (version 0.1.10)
{
  # A helpful description of the flake
  description = "Resonate: a dead simple programming model for modern applications";

  # Flake inputs
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/*";
    flake-schemas.url = "https://flakehub.com/f/DeterminateSystems/flake-schemas/*";
  };

  # Flake outputs that other flakes can use
  outputs = { self, nixpkgs, flake-schemas }:
    let
      # Helpers for producing system-specific outputs
      pkgsFor = system: import nixpkgs { inherit system; };
      supportedSystems = [ "x86_64-linux" "aarch64-darwin" "x86_64-darwin" "aarch64-linux" ];
      forEachSupportedSystem = f: nixpkgs.lib.genAttrs supportedSystems (system: f {
        pkgs = pkgsFor system;
      });

      # Global metadata (update this when the Resonate version changes)
      version = "0.5.0";
    in
    {
      # Schemas tell Nix about the structure of your flake's outputs
      inherit (flake-schemas) schemas;

      # Development environments
      devShells = forEachSupportedSystem ({ pkgs }: {
        default = pkgs.mkShell {
          # Pinned packages available in the environment
          packages = with pkgs; [
            # Go
            go_1_21
            gotools # goimports, godoc, etc.
            golangci-lint # Go linter

            # protoc
            protobuf

            # OpenAPI generator
            oapi-codegen

            # Nix formatter
            nixpkgs-fmt
          ];
        };
      });

      # Package outputs
      packages = forEachSupportedSystem ({ pkgs }: rec {
        # The Resonate server
        resonate = pkgs.buildGo121Module rec {
          pname = "resonate";
          inherit version;
          src = self;

          # A hash of all Go dependencies
          vendorHash = "sha256-Xpd+wVW5bFrRjzuhs9PsdB5zeyrFmR5k2vNCLe/e5Vs=";

          # Required for SQLite
          CGO_ENABLED = 1;

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

        # This enables you to use the shorthand `nix build` to build the server
        default = resonate;
      });

      # Docker image outputs
      dockerImages = forEachSupportedSystem ({ pkgs }: rec {
        # The Resonate server as an image
        resonate =
          let
            # A version of Nixpkgs solely for x86_64 Linux (the built image's system)
            linuxPkgs = pkgsFor "x86_64-linux";
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
