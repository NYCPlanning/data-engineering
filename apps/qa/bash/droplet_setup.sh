doctl compute droplet create \
    --image ubuntu-22-04-x64 \
    --size s-1vcpu-2gb-70gb-intel \
    --region nyc1 \
    --vpc-uuid b94e68d1-dc84-11e8-8650-3cfdfea9f8c8 \
    de-qaqc
