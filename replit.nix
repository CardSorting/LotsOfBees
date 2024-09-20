{ pkgs }: {
    deps = [
      pkgs.glibcLocales
        pkgs.python3
        pkgs.python310Packages.python-magic
        pkgs.file  # This package provides the libmagic library
    ];
}