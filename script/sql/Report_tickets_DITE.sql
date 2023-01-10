SELECT t.ID, t.`Titulo`, t.Estado, t.`Fecha de apertura`, t.`Solicitante - escritor`,
CASE WHEN t.user_key = 0 THEN t.alternative_email 
ELSE TRIM(CONCAT(us.firstname, ' ', us.realname )) END  "Solicitante - Solicitante" ,
t.`Notificaciones - Correo para el seguimiento`,
t.`Asignado a - Tecnico`, 
CASE WHEN UPPER(t.Cat_1) LIKE '%PERÚEDUCA%' THEN 'PerúEduca'
WHEN UPPER(t.Cat_1) LIKE '%CUENTAS WORKSPACE%' THEN 'CUENTAS GOOGLE WORKSPACE' 
WHEN UPPER(t.Cat_1) LIKE '%OTROS MINEDU%' THEN 'OTROS MINEDU'
WHEN UPPER(t.Cat_2) LIKE '%TECNOLÓGICA%' THEN 'TABLETAS' 
WHEN UPPER(t.Cat_2) LIKE '%PEDAGÓGICA%' THEN 'APRENDO EN CASA' 
ELSE 'SIN DEFINICION' END "Categoria",
t.`Ubicacion`,
 t.`Fuente de solicitud`, t.`Fecha de resolucion`
FROM
(
SELECT DISTINCT
       glpi_tickets.id AS ID,
       glpi_tickets.name AS "Titulo",
       CASE WHEN glpi_tickets.status = 1 THEN 'Nuevo'
       WHEN glpi_tickets.status = 2 THEN 'En curso (asignada)'
       WHEN glpi_tickets.status = 3 THEN 'En curso (planificado)'
       WHEN glpi_tickets.status = 4 THEN 'Pendiente'
       WHEN glpi_tickets.status = 5 THEN 'Resuelto' 
       WHEN glpi_tickets.status = 6 THEN 'Cerrado'
       ELSE glpi_tickets.status END AS "Estado",
       glpi_tickets.date_mod AS ITEM_Ticket_19,
       glpi_tickets.date AS "Fecha de apertura",
       TRIM(CONCAT(glpi_users_users_id_recipient.firstname, ' ', glpi_users_users_id_recipient.realname )) AS "Solicitante - escritor",
       glpi_tickets_users_tt1.users_id AS user_key,
       glpi_tickets_users_tt1.alternative_email,
		GROUP_CONCAT(DISTINCT CONCAT(glpi_tickets_users_tt1.users_id, ' ',
                                                        glpi_tickets_users_tt1.alternative_email)
                                                        SEPARATOR '$$##$$') AS ITEM_Ticket_4_2,
        glpi_tickets_users_tt1.alternative_email AS "Notificaciones - Correo para el seguimiento",
		CONCAT(glpi_users_users_id_lastupdater.firstname, ' ', glpi_users_users_id_lastupdater.realname) AS "Asignado a - Tecnico",
        TRIM(SUBSTRING_INDEX(glpi_itilcategories.completename, '>', 1)) AS "Cat_1",
		TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(glpi_itilcategories.completename , '>', 2), '>', -1)) AS "Cat_2",
        glpi_itilcategories.completename as "Categoria",
        glpi_locations.completename AS "Ubicacion",
        glpi_requesttypes.name AS "Fuente de solicitud",
        glpi_tickets.closedate AS "Fecha de resolucion"
        FROM glpi_tickets LEFT JOIN glpi_entities
                                          ON (glpi_tickets.entities_id = glpi_entities.id
                                              ) LEFT JOIN glpi_tickets_users  AS glpi_tickets_users_tt1
                                             ON (glpi_tickets.id = glpi_tickets_users_tt1.tickets_id
                                                 AND glpi_tickets_users_tt1.type = 1 )LEFT JOIN glpi_users  AS glpi_users_tt1
                                          ON (glpi_tickets_users_tt1.users_id = glpi_users_tt1.id
                                              ) LEFT JOIN glpi_tickets_users  AS glpi_tickets_users_tt2
                                             ON (glpi_tickets.id = glpi_tickets_users_tt2.tickets_id
                                                 AND glpi_tickets_users_tt2.type = 2 )LEFT JOIN glpi_users  AS glpi_users_tt2
                                          ON (glpi_tickets_users_tt2.users_id = glpi_users_tt2.id
                                              ) LEFT JOIN glpi_tickets_users  AS glpi_tickets_users_tt3
                                             ON (glpi_tickets.id = glpi_tickets_users_tt3.tickets_id
                                                 AND glpi_tickets_users_tt3.type = 3 )LEFT JOIN glpi_users  AS glpi_users_tt3
                                          ON (glpi_tickets_users_tt3.users_id = glpi_users_tt3.id
                                              )LEFT JOIN glpi_itilcategories
                                          ON (glpi_tickets.itilcategories_id = glpi_itilcategories.id
                                              )LEFT JOIN glpi_requesttypes
                                          ON (glpi_tickets.requesttypes_id = glpi_requesttypes.id
                                              )LEFT JOIN glpi_locations
                                          ON (glpi_tickets.locations_id = glpi_locations.id
                                              )LEFT JOIN glpi_users  AS glpi_users_users_id_recipient
                                          ON (glpi_tickets.users_id_recipient = glpi_users_users_id_recipient.id
                                              ) LEFT JOIN glpi_items_tickets
                                             ON (glpi_tickets.id = glpi_items_tickets.tickets_id
                                                 ) LEFT JOIN glpi_tickettasks
                                             ON (glpi_tickets.id = glpi_tickettasks.tickets_id
                                                  )LEFT JOIN glpi_users  AS glpi_users_tt4
                                          ON (glpi_tickettasks.users_id = glpi_users_tt4.id
                                              ) LEFT JOIN glpi_ticketsatisfactions
                                             ON (glpi_tickets.id = glpi_ticketsatisfactions.tickets_id
                                                 )LEFT JOIN glpi_olas  AS glpi_olas_olas_id_tt1
                                          ON (glpi_tickets.olas_id_ttr = glpi_olas_olas_id_tt1.id
                                              AND glpi_olas_olas_id_tt1.type = '0' )LEFT JOIN glpi_slas  AS glpi_slas_slas_id_tt1
                                          ON (glpi_tickets.slas_id_ttr = glpi_slas_slas_id_tt1.id
                                              AND glpi_slas_slas_id_tt1.type = '0' )LEFT JOIN glpi_users  AS glpi_users_users_id_lastupdater
                                          ON (glpi_tickets.users_id_lastupdater = glpi_users_users_id_lastupdater.id
                                              ) 
WHERE  glpi_tickets.is_deleted = 0  
AND (glpi_tickets.status IN ('1','2','3','4','5','6')
AND CAST(glpi_tickets.date AS DATE)  = CURDATE() - INTERVAL 1 DAY)
#BETWEEN (CURDATE() - INTERVAL 29 DAY) AND (CURDATE() - INTERVAL 15 DAY)) 
GROUP BY glpi_tickets.id 
) t LEFT JOIN glpi_users us ON t.user_key = us.id
ORDER BY t.ITEM_Ticket_19 DESC