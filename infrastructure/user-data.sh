#!/bin/bash
set -euo pipefail

systemctl daemon-reload
systemctl enable --now spot-data-pipeline.service
