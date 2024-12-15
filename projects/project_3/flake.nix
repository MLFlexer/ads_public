{
  description = "A Nix-flake-based Java development environment";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      javaVersion = 11; # Change this value to update the whole stack

      supportedSystems =
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSupportedSystem = f:
        nixpkgs.lib.genAttrs supportedSystems (system:
          f {
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ self.overlays.default ];
            };
          });
    in {
      overlays.default = final: prev: rec {
        jdk = prev."jdk${toString javaVersion}";
        maven = prev.maven.override { jdk_headless = jdk; };
      };

      devShells = forEachSupportedSystem ({ pkgs }: {
        default = pkgs.mkShell {
          packages = with pkgs;
            [ jdt-language-server spark hadoop scala_2_12 jdk maven python312 ]
            ++ (with pkgs.python312Packages; [
              matplotlib
              numpy
              pandas
              pyarrow
              fsspec
              huggingface-hub
              faker
            ]);
          shellHook = ''
            export SPARK_HOME=${pkgs.spark}
            export HADOOP_HOME=${pkgs.hadoop}
          '';
        };
      });
    };
}
