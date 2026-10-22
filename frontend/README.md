# EphemeralDB Flutter console

Flutter Web / Dart frontend for real database execution, traffic and recovery evidence.

From the project root, run `./scripts/start-console.ps1` to build the UI and start its
same-origin Python API at http://127.0.0.1:8765. See `../docs/console.md` for setup,
backend capabilities, authentication and experiment contracts.

Frontend checks:

```sh
flutter pub get
flutter analyze
flutter build web --release --no-wasm-dry-run
```

No cloud measurements or completed runs are hardcoded. Console tokens remain in memory.
