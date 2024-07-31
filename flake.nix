{
  description = "A clj-nix flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    clj-nix.url = "github:jlesquembre/clj-nix";
  };

  outputs = { self, nixpkgs, flake-utils, clj-nix }:

    flake-utils.lib.eachDefaultSystem (system: {
      devShells.default =
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in pkgs.mkShell {
          packages = [
            pkgs.openjdk21_headless
            pkgs.clojure
#             pkgs.duckdb
#             pkgs.parquet-tools
          ];
      };

      packages = {
        deps-lock = clj-nix.packages."${system}".deps-lock;
        default = clj-nix.lib.mkCljApp {
          pkgs = nixpkgs.legacyPackages.${system};
          modules = [
            # Option list:
            # https://jlesquembre.github.io/clj-nix/options/
            {
              projectSrc = ./.;
              name = "in.juspay/json-to-pqt";
              main-ns = "json-to-pqt/core";

              nativeImage.enable = true;

              # customJdk.enable = true;
            }
          ];
        };

      };
    });
}
