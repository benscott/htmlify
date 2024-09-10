# htmlify
Convert Scratchpads to HTML

pip install -e .

## Apache

https://www.digitalocean.com/community/tutorials/how-to-install-the-apache-web-server-on-ubuntu-22-04

## MYSQL

-- Create the new user
CREATE USER 'static'@'sp-static.nhm.ac.uk' IDENTIFIED BY 'bugFUV7d}Kangar00';

-- Grant all privileges on all databases
GRANT ALL PRIVILEGES ON *.* TO 'static'@'sp-static.nhm.ac.uk' WITH GRANT OPTION;

-- Apply the changes
FLUSH PRIVILEGES;



drush @hm sqlq "select nid, alias from hosting_site_alias where alias like '%myspecies.info' and alias not like 'www%'" > /tmp/static-site-aliases.csv

drush @hm sqlq "select hc.nid, hc.name as domain, hp.publish_path, hs.db_name, db.human_name from hosting_context hc inner join hosting_site hs on hs.nid=hc.nid inner join hosting_platform hp on hp.nid = hs.platform inner join hosting_server db on db.nid = hs.db_server where hs.status=1 order by hc.name" > /tmp/static-sites.csv 






