cd /home/ferdinand_harmel

#Install postgresql-client-12 
sudo apt install -y gnupg gnupg2 wget
echo deb http://apt.postgresql.org/pub/repos/apt/ buster-pgdg main > ./pgdg.list
sudo cp ~/pgdg.list /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt update
sudo apt install -y postgresql-client-12

#Import ssh private key
gsutil cp gs://pass-culture-data/compute_engine_dump_scalingo/private_rsa_key_vm.pem .
chmod 600 ./private_rsa_key_vm.pem 

#Add scalingo to known hosts
ssh-keyscan -H ssh.osc-fr1.scalingo.com >> ./.ssh/known_hosts

# Open SSH connection in background with a master controller
ssh -M -S ctrl_socket -fnNT -L 10000:pc-data-san-3937.postgresql.dbs.scalingo.com:31112 -i ./private_rsa_key_vm.pem git@ssh.osc-fr1.scalingo.com

#Export 
# pg_dump -U pc_data_san_3937 --section=pre-data --section=data --format=plain --no-owner --no-acl --table user --table provider --table offerer --table bank_information --table booking --table payment --table venue --table user_offerer --table offer --table stock --table favorite --table venue_type --table venue_label postgres://pc_data_san_3937:ku3g26m81EMUmYl4AYf_@localhost:10000/pc_data_san_3937 | sed -E 's/(DROP|CREATE|COMMENT ON) EXTENSION/-- \1 EXTENSION/g' > "./dump_$(date '+%Y_%m_%d').sql"
pg_dump -U pc_data_san_3937 --section=pre-data --section=data --format=plain --no-owner --no-acl postgres://pc_data_san_3937:ku3g26m81EMUmYl4AYf_@localhost:10000/pc_data_san_3937 | sed -E 's/(DROP|CREATE|COMMENT ON) EXTENSION/-- \1 EXTENSION/g; s/(CREATE|CREATE CONSTRAINT) TRIGGER/-- \1 TRIGGER/g' > "./dump_$(date '+%Y_%m_%d').sql"

#Close SSH connection
ssh -S ctrl_socket -O exit git@ssh.osc-fr1.scalingo.com

#If we want to restore directly from bash script 
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "DROP SCHEMA public CASCADE;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE SCHEMA public;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION postgis;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION unaccent;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -c "CREATE EXTENSION btree_gist;"
# psql -h 35.205.151.11 -U postgres -d test-restore-vm -f "dump_$(date '+%Y_%m_%d').sql"

#Export the file
gsutil cp "./dump_$(date '+%Y_%m_%d').sql" "gs://dump_scalingo_vm/dump_$(date '+%Y_%m_%d').sql"

#Remove it from the VM
rm "./dump_$(date '+%Y_%m_%d').sql"
