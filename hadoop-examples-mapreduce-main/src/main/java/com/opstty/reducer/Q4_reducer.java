package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class Q4_reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        // Initialiser la hauteur maximale à 0
        float maxHeight = 0.0f;

        // Parcourir toutes les hauteurs du genre d'arbre donné et trouver la plus grande
        for (FloatWritable value : values) {
            float height = value.get();
            if (height > maxHeight) {
                maxHeight = height;
            }
        }

        // Émettre le genre d'arbre et sa hauteur maximale
        context.write(key, new FloatWritable(maxHeight));
    }
}
