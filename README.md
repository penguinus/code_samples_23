project1 - fragment of Symfony 6 based project (PHP 8.1). 
Implements custom field for 
[EasyAdmin bundle](https://symfony.com/bundles/EasyAdminBundle/current/index.html) 
CRUD controller. The task was to implement file upload field for assets. 
Assets are splitted by virtual folders, structure of files and folders is defined in database 
and can be edited by admin. Field allows uploading to the folder predefined for category 
and updates related tables in DB. Implemented based on EasyAdmin API, extends it's base classes and interfaces.

project2 - fragment of Symfony 4 based project (PHP 5.6). System interacts with 
Google Ads (Adwords) as well as with Bing Ads API. The following set of classes 
implements interactions with Google and Bing API in common way (using OOP principles like extending common classes and interfaces) 
