package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class Q4_mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convertir la ligne de texte en une chaîne et la diviser en colonnes
        String line = value.toString();
        String[] columns = line.split(";");

        // Ignorer la première ligne (en-têtes de colonne)
        if (columns[0].equals("GEOPOINT")) {
            return;
        }

        // Extraire le genre et la hauteur de l'arbre
        String genre = columns[2];
        String heightStr = columns[6].trim();

        // Ignorer les lignes qui n'ont pas de hauteur valide
        if (heightStr.isEmpty()) {
            return;
        }

        // Convertir la hauteur en float
        float height = Float.parseFloat(heightStr);

        // Émettre la hauteur de l'arbre comme clé avec une valeur de 1
        context.write(new Text(genre), new FloatWritable(height));
    }
}
