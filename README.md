# Polar Bear

This repository contains the Polar Bear project.

## Prerequisites

Before you begin, ensure you have the following installed and updated:

- [Anaconda](https://www.anaconda.com/products/distribution)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/Cjaimesg/polarbear.git
   cd polarbear
   ```

2. Create an Anaconda environment with the specified requirements:
   ```
   conda create --name polarbear-env --file requirements.txt
   ```

3. Activate the environment:
   ```
   conda activate polarbear-env
   ```

## Configuration

1. Make a copy of the `secrets copy.toml` file and name it `secrets.toml`:
   ```
   cp "secrets copy.toml" secrets.toml
   ```

2. Edit the `secrets.toml` file and fill in the fields with your Snowflake credentials:

   ```toml
   [connections.snowflake]
   account = "your_account"
   user = "your_username"
   password = "your_password"
   role = "your_role"
   warehouse = "your_warehouse"
   database = "your_database"
   schema = "your_schema"
   client_session_keep_alive = true
   ```

   Make sure to replace the values in quotes with your actual credentials.

## Usage

[Add instructions on how to use the project once it's installed and configured]
