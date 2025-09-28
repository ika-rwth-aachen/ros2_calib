# Repository Guidelines

This project ships a PySide6 desktop app for offline ROS 2 calibration. Keep every contribution tightly scoped and verify changes against recorded `.mcap` rosbags before opening a pull request.

## Project Structure & Module Organization
Core logic lives in `ros2_calib/`: `main.py` wires the GUI, `calibration_widget.py` and `lidar2lidar_o3d_widget.py` host the calibration views, while helpers such as `bag_handler.py`, `calibration.py`, and `ros_utils.py` manage data access and math. GUI assets (icons, screenshots, demo media) reside in `assets/`. Build artifacts (`build/`, `dist/`, `ros2_calib.egg-info/`) are disposable—regenerate them locally instead of committing updates. Configuration and packaging metadata stay in `pyproject.toml` and `MANIFEST.in`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate`: set up an isolated toolchain.  
- `pip install -e .[dev]`: install the app plus linting/build extras.  
- `ruff check`: run linting (E, F, W, I) with a 100-character limit.  
- `ruff format`: auto-format Python sources; add `--check` in CI scripts.  
- `ros2_calib`: launch the GUI from your shell for quick smoke tests.  
- `python -m build`: produce wheels/sdists before publishing.

## Coding Style & Naming Conventions
Use Python 3.10+ features judiciously while keeping type hints for new APIs. Follow Ruff’s defaults: 4-space indentation, trailing commas where practical, and sorted imports. Modules and functions stay `snake_case`; Qt widgets/classes use `PascalCase`; constants stay upper snake. Break long GUI signal chains into helper functions and document non-obvious math with concise comments.

## Testing Guidelines
Automated tests are currently absent—add `pytest` modules alongside new features under `tests/` with files named `test_<feature>.py`. For GUI-heavy changes, describe manual validation: launch `ros2_calib`, load a representative `.mcap`, and confirm TF tree, calibrations, and exports behave as expected. Capture edge cases such as missing `/tf_static` frames or sparse point clouds.

## Commit & Pull Request Guidelines
Follow the existing short, imperative commit style (`add lidar2lidar calibrator`, `bump version to v0.0.6`). Group related changes and avoid bundled assets or builds. PRs should outline motivation, summarize implementation, list validation commands, and attach before/after screenshots for UI tweaks. Link issues when available and note any dependencies (e.g., new ROS bag schema) so reviewers can reproduce results quickly.
