update plant set municipality=(select city.id from municipality as city where city.inecode='25120')  where plant.name = 'Lleida'         ;
update plant set municipality=(select city.id from municipality as city where city.inecode='17148')  where plant.name = 'Riudarenes_SM'  ;
update plant set municipality=(select city.id from municipality as city where city.inecode='17148')  where plant.name = 'Riudarenes_BR'  ;
update plant set municipality=(select city.id from municipality as city where city.inecode='17148')  where plant.name = 'Riudarenes_ZE'  ;
update plant set municipality=(select city.id from municipality as city where city.inecode='08112')  where plant.name = 'Manlleu_Piscina';
update plant set municipality=(select city.id from municipality as city where city.inecode='08112')  where plant.name = 'Manlleu_Pavello';
update plant set municipality=(select city.id from municipality as city where city.inecode='25228')  where plant.name = 'Torrefarrera'   ;
update plant set municipality=(select city.id from municipality as city where city.inecode='46193')  where plant.name = 'Picanya'        ;
update plant set municipality=(select city.id from municipality as city where city.inecode='25230')  where plant.name = 'Torregrossa'    ;
update plant set municipality=(select city.id from municipality as city where city.inecode='47114')  where plant.name = 'Valteina'       ;
update plant set municipality=(select city.id from municipality as city where city.inecode='41006')  where plant.name = 'Alcolea'        ;
update plant set municipality=(select city.id from municipality as city where city.inecode='41055')  where plant.name = 'Matallana'      ;
update plant set municipality=(select city.id from municipality as city where city.inecode='05074')  where plant.name = 'Fontivsolar'    ;
update plant set municipality=(select city.id from municipality as city where city.inecode='41055')  where plant.name = 'Florida'        ;
update plant set municipality=(select city.id from municipality as city where city.inecode='04090')  where plant.name = 'Terborg'          ;
update plant set municipality=(select city.id from municipality as city where city.inecode='18152')  where plant.name = 'Llanillos'      ;
