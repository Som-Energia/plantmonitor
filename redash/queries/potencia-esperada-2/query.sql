select time, plant, expectedpower from view_expected_power
WHERE plant = {{ plant }} and time >= '{{ interval.start }}'
  AND time <= '{{ interval.end }}';