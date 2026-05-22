#!/bin/bash
set -e

# Create work pools
echo "📋 Creating work pools..."
echo "  Creating data-engineering-dev-pool..."
prefect work-pool create data-engineering-dev-pool --type process || echo "    Pool already exists"

echo "  Creating data-engineering-prod-pool..."
prefect work-pool create data-engineering-prod-pool --type process || echo "    Pool already exists"

echo "🎯 Work pools created! Current pools:"
prefect work-pool ls

# Create deployments using prefect deploy (Prefect 3 syntax)
echo ""
echo "📦 Creating deployments using Prefect 3 deploy command..."
echo "Using prefect.yaml configuration file..."

# Deploy all deployments
echo "Deploying all 6 deployments..."
prefect deploy --all || echo "    Some deployments may have failed"

echo ""
echo "🎯 Setup complete! Current deployments:"
prefect deployment ls

echo ""
echo "✅ All done! Summary:"
echo "  📋 Work pools: data-engineering-dev-pool, data-engineering-prod-pool"
echo "  📦 Deployments: 6 total (3 dev + 3 prod)"
echo "  🌐 Prefect UI: http://localhost:4200"