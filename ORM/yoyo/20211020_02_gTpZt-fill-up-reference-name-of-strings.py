"""
Fill up reference name of strings
Alcolea, Florida
"""

from yoyo import step

__depends__ = {'20211020_01_spih1-add-stringbox-name-of-strings'}

steps = [
    step('''update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string2', 'E2_CC1.3'),
                ('string3', 'E3_CC1.6'),
                ('string4', 'E4_CC1.5'),
                ('string5', 'E5_CC1.4'),
                ('string6', 'E6_CC1.7'),
                ('string7', 'E7_CC1.2'),
                ('string8', 'E8_CC1.1')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Alcolea'
            and inverter.name = 'inversor1'
            and string.name = sv.sv_string_name
            and s.id = string.id;

            update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string2', 'E2_CC1.7'),
                ('string3', 'E3_CC1.6'),
                ('string4', 'E4_CC1.5'),
                ('string5', 'E5_CC1.4'),
                ('string6', 'E6_CC1.3'),
                ('string7', 'E7_CC1.2'),
                ('string8', 'E8_CC1.1')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Alcolea'
            and inverter.name = 'inversor2'
            and string.name = sv.sv_string_name
            and s.id = string.id;

            update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string2', 'E2_CC3.3'),
                ('string3', 'E3_CC3.6'),
                ('string4', 'E4_CC3.5'),
                ('string5', 'E5_CC3.4'),
                ('string6', 'E6_CC3.7'),
                ('string7', 'E7_CC3.2'),
                ('string8', 'E8_CC3.1')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Alcolea'
            and inverter.name = 'inversor3'
            and string.name = sv.sv_string_name
            and s.id = string.id;

            update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string2', 'E2_CC4.7'),
                ('string3', 'E3_CC4.6'),
                ('string4', 'E4_CC4.5'),
                ('string5', 'E5_CC4.4'),
                ('string6', 'E6_CC4.3'),
                ('string7', 'E7_CC4.2'),
                ('string8', 'E8_CC4.1')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Alcolea'
            and inverter.name = 'inversor4'
            and string.name = sv.sv_string_name
            and s.id = string.id;

            update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string1', 'E1_CC1.1'),
                ('string2', 'E2_CC1.2'),
                ('string3', 'E3_CC1.3'),
                ('string4', 'E4_CC1.4'),
                ('string5', 'E5_CC1.5'),
                ('string9', 'E9_CC1.6'),
                ('string10', 'E10_CC1.7'),
                ('string11', 'E11_CC1.8'),
                ('string12', 'E12_CC1.9')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Florida'
            and inverter.name = 'inversor1'
            and string.name = sv.sv_string_name
            and s.id = string.id;

            update string s
              set stringbox_name = sv.sv_stringbox_name
            from (values
                ('string1', 'E1_CC2.1'),
                ('string2', 'E2_CC2.2'),
                ('string3', 'E3_CC2.3'),
                ('string4', 'E4_CC2.4'),
                ('string5', 'E5_CC2.5'),
                ('string10', 'E10_CC2.6'),
                ('string11', 'E11_CC2.7'),
                ('string12', 'E12_CC2.8'),
                ('string13', 'E13_CC2.9')
              ) as sv(sv_string_name, sv_stringbox_name)
            left join string on sv.sv_string_name = string.name
            left join inverter on inverter.id = string.inverter
            left join plant on plant.id = inverter.plant
            where plant.name = 'Florida'
            and inverter.name = 'inversor2'
            and string.name = sv.sv_string_name
            and s.id = string.id;
        ''','''
            update string s set stringbox_name = NULL
          from string so left join inverter on inverter.id = so.inverter
          left join plant on plant.id = inverter.plant
          where plant.name = 'Alcolea' or plant.name = 'Florida' and s.id = so.id;
        ''')
]
