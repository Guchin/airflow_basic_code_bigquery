
create or replace table operative_models.email_campaing as
select * from cleaning_tables.clientes
cross join raw_data.correos