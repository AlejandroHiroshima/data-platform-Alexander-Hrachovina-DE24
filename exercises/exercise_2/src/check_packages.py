import sys
import pkg_resources

# Print Python version
print(f"Python version: {sys.version}")

# Print installed packages
installed_packages = pkg_resources.working_set
packages = sorted([f"{pkg.key}=={pkg.version}" for pkg in installed_packages])
print("Installed packages:")
for package in packages:
    print(package)