R""(

# Examples

* Delete unreachable paths in the Nix store:

  ```console
  # nix store gc
  ```

* Delete up to 1 gigabyte of garbage:

  ```console
  # nix store gc --max 1G
  ```

* Fast incremental cleanup of old unused paths:

  ```console
  # nix store gc --delete-old-leafs 3600
  ```

# Description

This command deletes unreachable paths in the Nix store.

The `--delete-old-leafs` option deletes unused leaf paths (paths with no referrers) older
than the specified age in seconds. It runs multiple rounds to clean up dependency chains.

)""
