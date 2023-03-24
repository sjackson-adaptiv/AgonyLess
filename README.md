# An automation tool to interact with Adaptiv Networks 7.X CPEs

# Introduction
These tools were quickly cobbled together from an existing 7.X automation framework. 
The intention to provide operations a means to interact with a large number of CPEs, thus
reducing the agony of manually logging into every CPE to make changes.

While these scripts can easily be adapted for other uses, like CPE audits, configuration changes etc.
the current use case is to update the "monitor" and "admin" users passwords of many customer CPEs. 

Currently, the script will perform the following exact commands for each CPE IP listed:
 - login as monitor
 - admin
 - system
 - show version
 - show uptime
 - set password monitor <OLD_PASSWORD> <NEW_PASSWORD>
 - set password admin <OLD_PASSWORD> <NEW_PASSWORD>
 - exit
 - save config all

# Warning
By using these scripts you acknowledge and assume all risks and potential bad outcomes. While 
every effort was made to ensure things go smooth, use is YOUR responsibility.

These scripts come with no support and no liability towards their original author.

# Installation
 1. clone the repo
 2. pip install /path/to/working/copy

# How to use the scripts
Update the config.yml file
 - Add an IP entry for each CPE under network_entities. I would suggest rolling out in small
   smaller blocks of IPs. This is to mitigate the damage if something goes wrong.
 - Update the credentials section with the current usernames and passwords.
 - Update the new_passwords section with the new monitor and admin user passwords.

Run the script and pray to your favorite deity.
