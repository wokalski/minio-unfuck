{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {inherit system;};
      in {
        packages = {
          # derivation
        };
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            go gopls delve
            pkgs.cargo pkgs.rustc pkgs.rustfmt pkgs.rust-analyzer
            pkgs.duckdb
            pkgs.pkg-config
          ];
        };
      }
    );
}
