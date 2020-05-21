package de.hpi.des.hdes.engine.generators;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;

public class MustacheFactorySingleton {

    private static MustacheFactory instance;

    private MustacheFactorySingleton() {
    }

    public static synchronized MustacheFactory getInstance() {
        if (MustacheFactorySingleton.instance == null) {
            MustacheFactorySingleton.instance = new DefaultMustacheFactory();
        }
        return MustacheFactorySingleton.instance;
    }
}
