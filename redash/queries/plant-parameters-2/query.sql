select plant.name, plantmoduleparameters.* from plantmoduleparameters left join plant on plant.id = plantmoduleparameters.plant;
