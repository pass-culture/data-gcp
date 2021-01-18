# Declare variables
### Currently env api-staging ###
host=pass-culture-2892.postgresql.dbs.scalingo.com
port=30295
schema=pass_culture_2892
user=pass_culture_2892
password=Y1kW7O7fwnrfF8d6o_Kg

cd /home/data

# Open SSH connection in background with a master controller
sudo ssh -M -S ctrl_socket -fnNT -L 10000:$host:$port -i .ssh/id_rsa git@ssh.osc-fr1.scalingo.com

#Export 
# pg_dump -U pc_data_san_3937 --section=pre-data --section=data --format=plain --no-owner --no-acl --table user --table provider --table offerer --table bank_information --table booking --table payment --table venue --table user_offerer --table offer --table stock --table favorite --table venue_type --table venue_label postgres://pc_data_san_3937:ku3g26m81EMUmYl4AYf_@localhost:10000/pc_data_san_3937 | sed -E 's/(DROP|CREATE|COMMENT ON) EXTENSION/-- \1 EXTENSION/g' > "./dump_$(date '+%Y_%m_%d').sql"
sudo pg_dump -U $user --section=pre-data --section=data --format=plain --no-owner --no-acl postgres://$user:$password@localhost:10000/$schema | sed -E 's/(DROP|CREATE|COMMENT ON) EXTENSION/-- \1 EXTENSION/g; s/(CREATE|CREATE CONSTRAINT) TRIGGER/-- \1 TRIGGER/g' > "./dump_$(date '+%Y_%m_%d').sql"

#Close SSH connection
sudo ssh -S ctrl_socket -O exit git@ssh.osc-fr1.scalingo.com

#If we want to restore directly from bash script 
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "DROP SCHEMA public CASCADE;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE SCHEMA public;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION postgis;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION unaccent;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION btree_gist;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -f "dump_$(date '+%Y_%m_%d').sql"

#Export the file
sudo gsutil cp "./dump_$(date '+%Y_%m_%d').sql" "gs://dump_scalingo_vm/dump_$(date '+%Y_%m_%d').sql"

#Remove it from the VM
sudo rm "./dump_$(date '+%Y_%m_%d').sql"
