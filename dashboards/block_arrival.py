from dashboards.block_arrival import render

# Re-export the render function for backward compatibility
# This makes it transparent to consumers whether they're using
# the module or the package version

if __name__ == "__main__":
    render() 