# How to reproduce the project

## Step by step guide

1. If you don't have a Google Cloud Platform account, [create one now](https://console.cloud.google.com/freetrial/). This project can be reproduced using only the services included in the GCP [free tier](https://cloud.google.com/free/).

2. [Create a GCP Project](https://console.cloud.google.com/projectcreate).

3. Make sure that you have selected this new project.

4. Enable Google Compute Engine API for your project [in the GCP console](https://console.developers.google.com/apis/library/compute.googleapis.com).

5. [Create a service account key](https://console.cloud.google.com/apis/credentials/serviceaccountkey) with the following settings:
   - Select the project you created in the previous step.
   - Click "Create Service Account".
   - Give it any name you like and click "Create".
   - For the Role, choose "Project -> Editor", then click "Continue".
   - Skip granting additional users access, and click "Done".

6. After you create your service account, download your service account key.

   - Select your service account from the list.
   - Select the "Keys" tab.
   - In the drop down menu, select "Create new key".
   - Leave the "Key Type" as JSON.
   - Click "Create" to create the key and save the key file to your system.
   - Rename this key file to **google_credentials.json**

7. [Create an SSH key pair](https://cloud.google.com/compute/docs/connect/create-ssh-keys#create_an_ssh_key_pair) (if you haven't created it already).
   - KEY_FILENAME: the name for your SSH key file.
   - USERNAME: your username on the VM.

      ```bash
      ssh-keygen -t rsa -f ~/.ssh/<KEY_FILENAME> -C <USERNAME> -b 2048
      ```

8. [Add SSH keys to project metadata](https://cloud.google.com/compute/docs/connect/add-ssh-keys#add_ssh_keys_to_project_metadata).
      - In the Google Cloud console, go to the [Metadata](https://console.cloud.google.com/compute/metadata/sshKeys?_ga=2.84082073.1017998736.1680528409-313645582.1673880440) page.
      - Click the SSH keys tab.
      - Click Edit.
      - Click Add item. A text box opens.
      - Copy the contents of your public SSH key (```~/.ssh/<KEY_FILENAME>```).
      - Paste the contents in the text box (```ssh-rsa AAAAB3NzaC1yc2 ... UdMvQMCk= <USERNAME>```).
      - Click "Save"

9. [Create a GCP VM instance](https://console.cloud.google.com/compute/instancesAdd) with the following parameters.

   - **Name**: Instance name.
   - **Region**: europe-west6 (or choose another region)
   - **Zone**: europe-west6-a (or choose another zone)
   - **Machine configuration**:
     - **Series**: E2
     - **Machine type**: e2-standart-2 (2 vCPU, 8 GB memory)
   - **Boot disk**:
         - **Operating system**: Ubuntu
         - **Version**: Ubuntu 18.04 LTS
         - **Size** (GB): 20 GB
   - **Service account**: Choose the service account which was created in section 5

10. [Start your VM](https://console.cloud.google.com/compute/instances) and copy External IP.

11. Create or update an existing ssh configuration file:

    - HOST: the name of the host (you will use this name wnen connecting to the VM using ssh or sftp commands)
    - EXTERNAL_IP: external IP from GCP VM instances
    - KEY_FILENAME: the name for your SSH key file.
    - USERNAME: your username on the VM.

      ```bash
      touch ~/.ssh/config

      echo \
      '
      Host <HOST>
            HostName <EXTERNAL_IP>
            User <USERNAME>
            IdentityFile ~/.ssh/<KEY_FILENAME>
      ' >> ~/.ssh/config     
      ```

12. Copy your GCP service account key file to the VM using sftp.
      - HOST: the name of the host from ```~/.ssh/config```

      ```bash
      sftp <HOST>
      ```

      ```bash
      mkdir -p .google/credentials/
      ```

      ```bash
      cd .google/credentials/

      ```

      - GCP_SERVICE_ACCOUNT_KEY: a path to a key file which you have downloaded and renamed to "google_credentials.json" wnen creating a GCP service account in section 6

      ```bash
      put <GCP_SERVICE_ACCOUNT_KEY>
      ```

      ```bash
      exit
      ```

13. Connect to the VM using ssh.
      - HOST: the name of the host from ```~/.ssh/config```

      ```bash
      ssh <HOST>
      ```

14. Provide your service account credentials to Google Application Default Credentials.

      ```bash
      echo 'export GOOGLE_APPLICATION_CREDENTIALS="~/.google/credentials/google_credentials.json"' >> ~/.bashrc
      source ~/.bashrc
      gcloud auth application-default login 
      ```

15. Update, upgrade and install packages.

      ```bash
      sudo apt update && sudo apt upgrade
      sudo apt install wget gnome-keyring ca-certificates curl gnupg
      ```

16. [Install Terraform](https://developer.hashicorp.com/terraform/downloads).

      ```bash
      wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
      echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
      sudo apt update && sudo apt install terraform
      ```

17. [Install Docker](https://docs.docker.com/engine/install/ubuntu/).

      ```bash
      sudo mkdir -m 0755 -p /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

      echo \
      "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      "$(. /etc/os-release
      echo "$VERSION_CODENAME")" stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

      sudo chmod a+r /etc/apt/keyrings/docker.gpg
      sudo apt update
      sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      ```

18. [Install docker-compose](https://github.com/docker/compose#where-to-get-docker-compose).

      ```bash
      mkdir -p "${HOME}/.docker/cli-plugins" && cd "${HOME}/.docker/cli-plugins"
      wget https://github.com/docker/compose/releases/download/v2.17.2/docker-compose-linux-x86_64 -O docker-compose
      chmod +x docker-compose
      echo 'export PATH="${PATH}:${HOME}/.docker/cli-plugins"' >> ~/.bashrc
      source ~/.bashrc
      ```

19. [Linux post-installation steps for Docker](https://docs.docker.com/engine/install/linux-postinstall/).

      ```bash
      sudo groupadd docker
      sudo usermod -aG docker $USER
      ```

      ```bash
      sudo reboot
      ```

      ```bash
      ssh <HOST>
      ```

      ```bash
      sudo service docker restart
      ```

20. Clone the repo and run some initial commands.

      ```bash
      git clone https://github.com/aeryuzhev/de-zoomcamp-project.git
      cd de-zoomcamp-project
      echo -e "AIRFLOW_UID=$(id -u)" > .env
      mkdir -p data plugins logs
      ```

21. Run Airflow in Docker.

      ```bash
      docker-compose build
      docker-compose up -d
      ```